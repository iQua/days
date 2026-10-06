//! P16 H3 (aicb): the AICB trace grammar and its refusals (days-gpu `evidence/P16/aicb-design.md`
//! §1). The expected counts are the ones `aicb_plan.py` printed for the same files
//! (`evidence/P16/aicb-design/baseline/aicb-plan.txt`).

use std::collections::BTreeMap;
use std::path::PathBuf;

use days::workload::aicb::{Algorithm, Column, Comm, GroupKind, Header, Trace, parse_trace};

fn fixture(name: &str) -> String {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/aicb")
        .join(name);
    std::fs::read_to_string(&path).unwrap_or_else(|error| panic!("{}: {error}", path.display()))
}

fn column_counts(trace: &Trace) -> BTreeMap<(Column, Algorithm, GroupKind), usize> {
    let mut counts = BTreeMap::new();
    for record in &trace.records {
        for column in [
            Column::Forward,
            Column::InputGradient,
            Column::WeightGradient,
        ] {
            if let Some(Comm { algorithm, group }) = record.column(column).comm {
                *counts.entry((column, algorithm, group)).or_default() += 1;
            }
        }
    }
    counts
}

#[test]
fn b4_dense_trace_parses_as_simai_reads_it() {
    let trace = parse_trace(&fixture("b4-gpt13b-w128-tp8-pp2-gbs8.txt")).expect("b4 parses");
    assert_eq!(
        trace.header,
        Header {
            tp: 8,
            ep: 1,
            pp: 2,
            vpp: 20,
            ga: 1,
            all_gpus: 128,
            pp_comm_bytes: 5_242_880,
        }
    );
    assert_eq!(trace.records.len(), 93);
    let first = &trace.records[0];
    assert_eq!(first.name, "grad_norm");
    assert_eq!(first.line, 3);
    assert_eq!(first.forward.compute_ns, 1);
    assert_eq!(
        first.forward.comm,
        Some(Comm {
            algorithm: Algorithm::AllGather,
            group: GroupKind::Tp
        })
    );
    assert_eq!(first.forward.size_bytes, 1_942_896_640);
    assert_eq!(first.input_gradient.comm, None);
    assert_eq!(
        first.weight_gradient.comm,
        Some(Comm {
            algorithm: Algorithm::ReduceScatter,
            group: GroupKind::Dp
        })
    );
    assert_eq!(first.weight_gradient.size_bytes, 3_885_793_280);
    assert_eq!(first.process_time_ns, 100);
    // moe_grad_norm1: a DP_EP all-gather of 0 B (elided later, not refused here).
    assert_eq!(
        trace.records[1].weight_gradient.comm,
        Some(Comm {
            algorithm: Algorithm::AllGather,
            group: GroupKind::DpEp
        })
    );
    assert_eq!(trace.records[1].weight_gradient.size_bytes, 0);
    let counts = column_counts(&trace);
    use Algorithm::*;
    use Column::*;
    use GroupKind::*;
    assert_eq!(
        counts,
        BTreeMap::from([
            ((Forward, AllReduce, Tp), 8),
            ((Forward, AllGather, Tp), 42),
            ((Forward, ReduceScatter, Tp), 40),
            ((WeightGradient, AllGather, DpEp), 1),
            ((WeightGradient, ReduceScatter, Dp), 1),
            ((WeightGradient, ReduceScatter, DpEp), 1),
        ])
    );
    let last = trace.records.last().unwrap();
    assert_eq!((last.name.as_str(), last.line), ("optimizer4", 95));
    assert_eq!(last.forward.compute_ns, 0);
    assert_eq!(last.forward.size_bytes, 4);
}

#[test]
fn smoke_moe_trace_parses_as_simai_reads_it() {
    let trace = parse_trace(&fixture("smoke-moe-w128-tp2-ep32.txt")).expect("smoke parses");
    assert_eq!(
        trace.header,
        Header {
            tp: 2,
            ep: 32,
            pp: 1,
            vpp: 12,
            ga: 2,
            all_gpus: 128,
            pp_comm_bytes: 0,
        }
    );
    assert_eq!(trace.records.len(), 184);
    let counts = column_counts(&trace);
    use Algorithm::*;
    use Column::*;
    use GroupKind::*;
    assert_eq!(
        counts,
        BTreeMap::from([
            ((Forward, AllReduce, Tp), 9),
            ((Forward, AllGather, Tp), 75),
            ((Forward, ReduceScatter, Tp), 48),
            ((Forward, AllToAll, Ep), 48),
            ((InputGradient, AllGather, Tp), 24),
            ((InputGradient, ReduceScatter, Tp), 24),
            ((InputGradient, AllToAll, Ep), 48),
            ((WeightGradient, AllGather, DpEp), 1),
            ((WeightGradient, ReduceScatter, Dp), 1),
            ((WeightGradient, ReduceScatter, DpEp), 1),
        ])
    );
}

/// The flagship trace is an artifact, not a fixture (`DAYS_AICB_FLAGSHIP`).
#[test]
#[ignore = "needs DAYS_AICB_FLAGSHIP (days-gpu evidence/P16/collops-design/traces/flagship-tp2-ep32-w1024.txt)"]
fn flagship_trace_parses() {
    let path = std::env::var("DAYS_AICB_FLAGSHIP").expect("DAYS_AICB_FLAGSHIP");
    let trace = parse_trace(&std::fs::read_to_string(path).unwrap()).expect("flagship parses");
    assert_eq!(
        (trace.header.tp, trace.header.ep, trace.header.ga),
        (2, 32, 2)
    );
    assert_eq!(trace.header.all_gpus, 1024);
    assert_eq!(trace.records.len(), 688);
}

const HEADER: &str = "HYBRID_TRANSFORMER_FWD_IN_BCKWD model_parallel_NPU_group: 2 ep: 1 pp: 1 \
vpp: 1 ga: 1 all_gpus: 16 checkpoints: 0 checkpoint_initiates: 0 pp_comm: 0";
const RECORD: &str = "layer\t-1\t1\tALLGATHER\t4096\t1\tNONE\t0\t1\tREDUCESCATTER\t8192\t100";

fn trace_text(header: &str, count: &str, records: &[&str]) -> String {
    let mut text = format!("{header}\n{count}\n");
    for record in records {
        text.push_str(record);
        text.push('\n');
    }
    text
}

fn refused(text: &str) -> (Option<usize>, String) {
    let error = parse_trace(text).expect_err("refused");
    (error.line, error.message)
}

#[test]
fn a_minimal_trace_parses_and_keeps_whitespace_lenient_records() {
    let trace = parse_trace(&trace_text(HEADER, "1", &[RECORD])).expect("parses");
    assert_eq!(trace.records.len(), 1);
    // SimAI reads records with `>>`: spaces and tabs both separate fields, blank lines are skipped.
    let spaced = "layer -1 1 ALLGATHER 4096 1 NONE 0 1 REDUCESCATTER 8192 100";
    let trace = parse_trace(&trace_text(HEADER, "1", &["", spaced, ""])).expect("parses");
    assert_eq!(trace.records[0].line, 4);
    // AICB writes `pp_comm` as a float; a zero fraction is exact.
    let header = HEADER
        .replace("pp: 1", "pp: 2")
        .replace("pp_comm: 0", "pp_comm: 5242880.0");
    let trace = parse_trace(&trace_text(&header, "1", &[RECORD])).expect("parses");
    assert_eq!(trace.header.pp_comm_bytes, 5_242_880);
}

#[test]
fn header_refusals_name_line_one() {
    let cases: Vec<(String, &str)> = vec![
        (
            HEADER.replace("HYBRID_TRANSFORMER_FWD_IN_BCKWD", "HYBRID_TRANSFORMER"),
            "parallelism policy",
        ),
        (HEADER.replace(" ep: 1", ""), "missing header key `ep:`"),
        (format!("{HEADER} ga: 1"), "duplicate header key `ga:`"),
        (format!("{HEADER} bogus: 3"), "unknown header key `bogus:`"),
        (format!("{HEADER} vpp:"), "has no value"),
        (
            HEADER.replace("checkpoints: 0", "checkpoints: 1"),
            "checkpoints",
        ),
        (
            HEADER.replace("checkpoint_initiates: 0", "checkpoint_initiates: 2"),
            "checkpoint_initiates",
        ),
        (HEADER.replace("pp: 1", "pp: 2"), "pp = 2 with pp_comm = 0"),
        (
            HEADER.replace("pp_comm: 0", "pp_comm: 64"),
            "pp = 1 with pp_comm = 64",
        ),
        (
            HEADER
                .replace("pp: 1", "pp: 2")
                .replace("pp_comm: 0", "pp_comm: 5242880.5"),
            "not an exact non-negative integer",
        ),
        (HEADER.replace("ga: 1", "ga: 0"), "ga must be positive"),
        (
            HEADER.replace("all_gpus: 16", "all_gpus: 2147483648"),
            "above 2147483647",
        ),
        (
            HEADER.replace("ep: 1", "ep: -1"),
            "not an exact non-negative integer",
        ),
        (
            HEADER.replace("ep: 1", "ep: 01"),
            "not an exact non-negative integer",
        ),
    ];
    for (header, expected) in cases {
        let (line, message) = refused(&trace_text(&header, "1", &[RECORD]));
        assert_eq!(line, Some(1), "{header}: {message}");
        assert!(
            message.contains(expected),
            "{header}: `{message}` lacks `{expected}`"
        );
    }
}

#[test]
fn record_refusals_name_their_line() {
    let cases: Vec<(String, Vec<String>, Option<usize>, &str)> = vec![
        (
            "2".into(),
            vec![RECORD.into()],
            Some(2),
            "declares 2 records, the file has 1",
        ),
        (
            "1".into(),
            vec![RECORD.into(), RECORD.into()],
            Some(2),
            "declares 1 records, the file has 2",
        ),
        ("x".into(), vec![RECORD.into()], Some(2), "record count"),
        (
            "1".into(),
            vec![format!("{RECORD}\t7")],
            Some(3),
            "13 fields, expected 12",
        ),
        (
            "1".into(),
            vec![RECORD.replace("\t100", "")],
            Some(3),
            "11 fields, expected 12",
        ),
        (
            "1".into(),
            vec![RECORD.replace("\t-1\t", "\t0\t")],
            Some(3),
            "depen",
        ),
        (
            "1".into(),
            vec![RECORD.replace("\t4096\t", "\t4096.5\t")],
            Some(3),
            "fp_comm_size",
        ),
        (
            "1".into(),
            vec![RECORD.replace("layer\t-1\t1\t", "layer\t-1\t1.5\t")],
            Some(3),
            "fp_compute",
        ),
        (
            "1".into(),
            vec![RECORD.replace("\t100", "\t-100")],
            Some(3),
            "process_time",
        ),
        (
            "1".into(),
            vec![RECORD.replace("\t8192\t", "\t18446744073709551616\t")],
            Some(3),
            "wg_comm_size",
        ),
        (
            "1".into(),
            vec![RECORD.replace("ALLGATHER", "BROADCAST")],
            Some(3),
            "unknown fp comm type `BROADCAST`",
        ),
        (
            "1".into(),
            vec![RECORD.replace("ALLGATHER", "ALLGATHER_TP")],
            Some(3),
            "unknown fp comm type `ALLGATHER_TP`",
        ),
        (
            "1".into(),
            vec![RECORD.replace("ALLGATHER", "ALLREDUCEALLTOALL")],
            Some(3),
            "ALLREDUCEALLTOALL",
        ),
        (
            "1".into(),
            vec![RECORD.replace("REDUCESCATTER", "ALLREDUCEALLTOALL_EP")],
            Some(3),
            "ALLREDUCEALLTOALL",
        ),
        (
            "1".into(),
            vec![RECORD.replace("NONE", "none")],
            Some(3),
            "unknown ig comm type `none`",
        ),
        (
            "1".into(),
            vec![RECORD.replace("layer", "lay\u{e9}r")],
            None,
            "not ASCII",
        ),
    ];
    for (count, records, line, expected) in cases {
        let records: Vec<&str> = records.iter().map(String::as_str).collect();
        let (got_line, message) = refused(&trace_text(HEADER, &count, &records));
        assert_eq!(got_line, line, "{records:?}: {message}");
        assert!(
            message.contains(expected),
            "{records:?}: `{message}` lacks `{expected}`"
        );
    }
}

#[test]
fn column_types_map_to_groups_as_simai_does() {
    // fp and ig: bare -> TP; wg: bare -> DP; `_EP` -> EP and `_DP_EP` -> DP_EP in every column.
    let record = "r\t-1\t1\tALLTOALL\t64\t1\tALLREDUCE_EP\t64\t1\tALLREDUCE\t64\t100";
    let trace = parse_trace(&trace_text(HEADER, "1", &[record])).expect("parses");
    let r = &trace.records[0];
    assert_eq!(r.forward.comm.unwrap().group, GroupKind::Tp);
    assert_eq!(r.forward.comm.unwrap().algorithm, Algorithm::AllToAll);
    assert_eq!(r.input_gradient.comm.unwrap().group, GroupKind::Ep);
    assert_eq!(r.weight_gradient.comm.unwrap().group, GroupKind::Dp);
    let record = "r\t-1\t1\tREDUCESCATTER_DP_EP\t64\t1\tALLGATHER\t64\t1\tALLTOALL_EP\t64\t100";
    let trace = parse_trace(&trace_text(HEADER, "1", &[record])).expect("parses");
    let r = &trace.records[0];
    assert_eq!(r.forward.comm.unwrap().group, GroupKind::DpEp);
    assert_eq!(r.input_gradient.comm.unwrap().group, GroupKind::Tp);
    assert_eq!(
        r.weight_gradient.comm.unwrap(),
        Comm {
            algorithm: Algorithm::AllToAll,
            group: GroupKind::Ep
        }
    );
}

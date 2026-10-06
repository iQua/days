//! P16 H3 (aicb): the per-rank schedule SimAI runs for a trace (days-gpu
//! `evidence/P16/aicb-design.md` §3): SimAI's size rules in SimAI's order, the wire-size hang
//! gate, fused delay chains (A1), the data queue's LIFO order (R9), the realized start order the
//! ECMP ordinals follow (A7), the imbalanced arm's matrices (R7), and Megatron's pipeline
//! transfers. The expected numbers for the committed traces are the ones `aicb_plan.py` printed
//! (`evidence/P16/aicb-design/baseline/aicb-plan.txt`).

use std::collections::BTreeMap;
use std::path::PathBuf;

use days::workload::aicb::{
    Algorithm, ChainItem, Column, DataQueueOrder, ExpertRouting, Fidelity, GroupKind,
    ImbalanceParams, PipelineTransfer, Plan, PlanOptions, PropagationBounds, SegmentEnd, SimaiEnv,
    StartItem, Trace, form_groups, in_hang_window, parse_trace, plan_schedule,
};

const B4: &str = "b4-gpt13b-w128-tp8-pp2-gbs8.txt";
const SMOKE: &str = "smoke-moe-w128-tp2-ep32.txt";
const SIMAI_ENV: SimaiEnv = SimaiEnv {
    send_lat_us: 3,
    nvls_enable: true,
    pxn_enable: false,
};
const BOUNDS_100G: PropagationBounds = PropagationBounds {
    link_delay_ns: 500,
    nic_rate_bps: 100_000_000_000,
    nvlink_delay_ns: 25,
};

fn fixture(name: &str) -> Trace {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/aicb")
        .join(name);
    parse_trace(&std::fs::read_to_string(path).unwrap()).unwrap()
}

fn options(fidelity: Fidelity, gpu_type: &str) -> PlanOptions {
    PlanOptions {
        fidelity,
        expert_routing: ExpertRouting::Uniform,
        mtu_bytes: 9000,
        gpu_type: gpu_type.to_owned(),
        simai_env: Some(SIMAI_ENV),
    }
}

fn planned(trace: &Trace, options: &PlanOptions) -> Plan {
    try_plan(trace, options).unwrap()
}

fn try_plan(trace: &Trace, options: &PlanOptions) -> Result<Plan, String> {
    let groups = form_groups(&trace.header, options.fidelity, 8).map_err(|e| e.to_string())?;
    plan_schedule(trace, &groups, options, &BOUNDS_100G).map_err(|e| e.to_string())
}

/// Gaps between consecutive non-delay chain items, as `aicb_plan.py` prints them.
fn gap_histogram(items: &[ChainItem]) -> BTreeMap<u64, usize> {
    let mut histogram = BTreeMap::new();
    let mut run = 0;
    let mut seen = false;
    for item in items {
        if let ChainItem::Delay(ns) = item {
            run += ns;
        } else {
            if seen {
                *histogram.entry(run).or_default() += 1;
            }
            seen = true;
            run = 0;
        }
    }
    histogram
}

fn data_names(trace: &Trace, plan: &Plan, ops: &[usize]) -> Vec<String> {
    ops.iter()
        .map(|&op| trace.records[plan.ops[op].record].name.clone())
        .collect()
}

fn message_totals(trace: &Trace, plan: &Plan) -> (u64, u64, u64) {
    let groups = form_groups(&trace.header, plan.fidelity, 8).unwrap();
    let (mut network, mut nvlink, mut notify) = (0, 0, 0);
    for op in &plan.ops {
        let instances = groups.family(op.group).len() as u64;
        network += instances * op.network_messages_per_group();
        let nv = instances * op.nvlink_messages_per_group();
        nvlink += nv;
        if !op.single_server() {
            notify += nv;
        }
    }
    (network, nvlink, notify)
}

#[test]
fn smoke_schedule_matches_the_design_numbers() {
    let trace = fixture(SMOKE);
    let plan = planned(&trace, &options(Fidelity::Simai, "A100"));
    assert_eq!(plan.ops.len(), 279);
    assert_eq!(plan.counters.fp_clamps, 4);
    assert_eq!(plan.counters.elided_zero_wg, 0);
    assert_eq!(plan.counters.hang_window_recorded, 0);
    assert_eq!(message_totals(&trace, &plan), (352_384, 109_440, 61_056));
    let a2a = plan
        .ops
        .iter()
        .find(|op| op.algorithm == Algorithm::AllToAll)
        .unwrap();
    assert_eq!(
        (a2a.message_bytes, a2a.group_size, a2a.servers),
        (2_097_152, 32, 8)
    );
    let dp = plan
        .ops
        .iter()
        .find(|op| op.group == GroupKind::Dp)
        .unwrap();
    assert_eq!(
        (dp.message_bytes, dp.channels, dp.steps),
        (15_049_472, 4, 63)
    );
    // The optimizer's 4 B all-reduce is clamped to 4,096 B: 1,024 B per message on TP2.
    let optimizer = plan
        .ops
        .iter()
        .rev()
        .find(|op| op.column == Column::Forward)
        .unwrap();
    assert_eq!(
        (optimizer.total_bytes, optimizer.message_bytes),
        (4096, 1024)
    );
    assert_eq!(plan.stages.len(), 1);
    let stage = &plan.stages[0];
    assert_eq!(stage.segments.len(), 99, "fused segments per rank");
    assert_eq!(
        stage.unfused_stages, 459,
        "unfused compute and delay stages per rank"
    );
    assert_eq!(
        gap_histogram(&stage.items),
        BTreeMap::from([
            (4, 2),
            (101, 7),
            (102, 170),
            (104, 73),
            (108, 1),
            (116, 22),
            (122, 1),
            (124, 1),
            (128, 1)
        ])
    );
    let queue = &plan.data_queue;
    assert_eq!(
        data_names(&trace, &plan, &queue.issued),
        ["moe_grad_norm2", "moe_grad_norm1", "grad_norm"]
    );
    assert_eq!(queue.issue_offsets_ns, [Some(0), Some(4), Some(8)]);
    assert_eq!(queue.head_lower_bound_ns, 12_080_596);
    assert_eq!(queue.kind, DataQueueOrder::Lifo);
    assert_eq!(
        data_names(&trace, &plan, &queue.order),
        ["moe_grad_norm2", "grad_norm", "moe_grad_norm1"]
    );
    assert!(plan.ecmp_ordinals_exact);
    // The start order holds the cross-host model ops in chain order, then the data ops in LIFO
    // order, and no single-server op.
    let ops: Vec<usize> = plan
        .start_order
        .iter()
        .map(|item| match item {
            StartItem::Collective(op) => *op,
            StartItem::Pipeline(_) => panic!("no pipeline at PP 1"),
        })
        .collect();
    assert_eq!(ops.len(), 96 + 3);
    assert!(ops.iter().all(|&op| !plan.ops[op].single_server()));
    assert_eq!(&ops[96..], &queue.order[..]);
    // Fused segments end at the cross-host points: 96 all-to-alls and 3 forks.
    let forks = stage
        .segments
        .iter()
        .filter(|segment| matches!(segment.end, SegmentEnd::Fork(_)))
        .count();
    assert_eq!(forks, 3);
}

#[test]
fn b4_schedule_is_one_fused_segment_then_the_dp_ring() {
    let trace = fixture(B4);
    let plan = planned(&trace, &options(Fidelity::Simai, "A100"));
    assert_eq!(plan.ops.len(), 91);
    assert_eq!(plan.counters.elided_zero_wg, 2);
    assert_eq!(plan.counters.fp_clamps, 4);
    assert_eq!(message_totals(&trace, &plan), (1_920, 702_464, 0));
    let stage = &plan.stages[0];
    assert_eq!(stage.segments.len(), 1);
    assert_eq!(stage.segments[0].single_server_ops.len(), 90);
    assert_eq!(stage.unfused_stages, 181);
    assert_eq!(
        gap_histogram(&stage.items),
        BTreeMap::from([(101, 7), (102, 81), (108, 1), (458, 1)])
    );
    assert_eq!(plan.data_queue.kind, DataQueueOrder::Fifo);
    assert_eq!(
        data_names(&trace, &plan, &plan.data_queue.order),
        ["grad_norm"]
    );
    let dp = &plan.ops[plan.data_queue.order[0]];
    assert_eq!(
        (dp.message_bytes, dp.channels, dp.servers),
        (242_862_080, 1, 16)
    );
    assert!(plan.ecmp_ordinals_exact);
}

#[test]
#[ignore = "needs DAYS_AICB_FLAGSHIP (days-gpu evidence/P16/collops-design/traces/flagship-tp2-ep32-w1024.txt)"]
fn flagship_schedule_matches_the_design_numbers() {
    let path = std::env::var("DAYS_AICB_FLAGSHIP").expect("DAYS_AICB_FLAGSHIP");
    let trace = parse_trace(&std::fs::read_to_string(path).unwrap()).unwrap();
    let plan = planned(&trace, &options(Fidelity::Simai, "H100"));
    assert_eq!(plan.ops.len(), 1071);
    assert_eq!(
        message_totals(&trace, &plan),
        (11_564_032, 4_168_704, 2_749_440)
    );
    assert_eq!(plan.stages[0].segments.len(), 387);
    assert_eq!(plan.stages[0].unfused_stages, 1755);
    let bounds = PropagationBounds {
        nic_rate_bps: 400_000_000_000,
        ..BOUNDS_100G
    };
    let groups = form_groups(&trace.header, Fidelity::Simai, 8).unwrap();
    let plan = plan_schedule(&trace, &groups, &options(Fidelity::Simai, "H100"), &bounds).unwrap();
    assert_eq!(plan.data_queue.head_lower_bound_ns, 22_664_250);
    assert_eq!(
        data_names(&trace, &plan, &plan.data_queue.order),
        ["moe_grad_norm2", "grad_norm", "moe_grad_norm1"]
    );
    assert!(plan.ecmp_ordinals_exact);
}

#[test]
fn imbalanced_routing_pairs_dispatch_and_combine() {
    let trace = fixture(SMOKE);
    let routing = ExpertRouting::Imbalanced(ImbalanceParams {
        seed: 11,
        experts: 128,
        topk: 8,
        tokens_per_rank: 2048,
        zipf: 1,
    });
    let simai = PlanOptions {
        expert_routing: routing,
        ..options(Fidelity::Simai, "A100")
    };
    assert!(
        try_plan(&trace, &simai)
            .unwrap_err()
            .contains("Days-only arm")
    );
    let megatron = PlanOptions {
        expert_routing: routing,
        ..options(Fidelity::Megatron, "A100")
    };
    let plan = planned(&trace, &megatron);
    let forward: Vec<_> = plan
        .ops
        .iter()
        .filter(|op| op.algorithm == Algorithm::AllToAll && op.column == Column::Forward)
        .map(|op| op.matrix.unwrap())
        .collect();
    assert_eq!(forward.len(), 48);
    for (index, matrix) in forward.iter().enumerate() {
        assert_eq!(matrix.matrix, index as u32 / 2);
        assert_eq!(matrix.transpose, index % 2 == 1, "dispatch M, combine M^T");
        assert_eq!(matrix.bytes_per_copy, 4096);
    }
    for op in plan
        .ops
        .iter()
        .filter(|op| op.column == Column::InputGradient && op.algorithm == Algorithm::AllToAll)
    {
        let fp = plan
            .ops
            .iter()
            .find(|other| other.record == op.record && other.column == Column::Forward)
            .unwrap();
        let (ig, fp) = (op.matrix.unwrap(), fp.matrix.unwrap());
        assert_eq!((ig.matrix, ig.transpose), (fp.matrix, !fp.transpose));
    }
    for (params, expected) in [
        (
            ImbalanceParams {
                experts: 100,
                ..params_of(routing)
            },
            "do not divide over the 32-rank",
        ),
        (
            ImbalanceParams {
                tokens_per_rank: 3000,
                ..params_of(routing)
            },
            "copies of a whole number",
        ),
        (
            ImbalanceParams {
                zipf: 2,
                ..params_of(routing)
            },
            "zipf must be 0 or 1",
        ),
    ] {
        let options = PlanOptions {
            expert_routing: ExpertRouting::Imbalanced(params),
            ..options(Fidelity::Megatron, "A100")
        };
        let error = try_plan(&trace, &options).unwrap_err();
        assert!(error.contains(expected), "`{error}` lacks `{expected}`");
    }
}

fn params_of(routing: ExpertRouting) -> ImbalanceParams {
    match routing {
        ExpertRouting::Imbalanced(params) => params,
        ExpertRouting::Uniform => unreachable!(),
    }
}

#[test]
fn megatron_pipeline_transfers_sit_at_block_boundaries() {
    let trace = fixture(B4);
    let plan = planned(&trace, &options(Fidelity::Megatron, "A100"));
    assert_eq!(plan.stages.len(), 2);
    let ends = |stage: usize| -> Vec<SegmentEnd> {
        plan.stages[stage]
            .segments
            .iter()
            .map(|segment| segment.end)
            .collect()
    };
    let fwd = PipelineTransfer {
        boundary: 0,
        block: 0,
        backward: false,
    };
    let bwd = PipelineTransfer {
        boundary: 0,
        block: 0,
        backward: true,
    };
    let fork = SegmentEnd::Fork(plan.data_queue.order[0]);
    assert_eq!(
        ends(0),
        [SegmentEnd::Send(fwd), SegmentEnd::Receive(bwd), fork]
    );
    assert_eq!(
        ends(1),
        [SegmentEnd::Receive(fwd), SegmentEnd::Send(bwd), fork]
    );
    // The receive of stage 1 waits after the prologue: the first three records' forward items.
    let receive = plan.stages[1]
        .items
        .iter()
        .position(|item| *item == ChainItem::Receive(fwd))
        .unwrap();
    assert!(
        plan.stages[1].items[..receive]
            .iter()
            .all(|item| !matches!(item, ChainItem::Collective(op) if plan.ops[*op].record >= 3))
    );
    let pipeline: Vec<_> = plan
        .start_order
        .iter()
        .filter_map(|item| match item {
            StartItem::Pipeline(transfer) => Some(*transfer),
            StartItem::Collective(_) => None,
        })
        .collect();
    assert_eq!(pipeline, [fwd, bwd]);
    assert_eq!(plan.counters.hang_window_recorded, 0);
    assert!(plan.ecmp_ordinals_exact);
}

const HEADER16: &str = "HYBRID_TRANSFORMER_FWD_IN_BCKWD model_parallel_NPU_group: 2 ep: 8 pp: 1 \
vpp: 1 ga: 1 all_gpus: 16 checkpoints: 0 checkpoint_initiates: 0 pp_comm: 0";

fn synthetic(records: &[&str]) -> Trace {
    let mut text = format!("{HEADER16}\n{}\n", records.len());
    for record in records {
        text.push_str(record);
        text.push('\n');
    }
    parse_trace(&text).unwrap()
}

#[test]
fn the_hang_gate_refuses_under_simai_and_records_under_megatron() {
    assert!(!in_hang_window(1_472, 9000));
    assert!(in_hang_window(1, 9000), "1 B: a 53 B packet");
    assert!(in_hang_window(8, 9000));
    assert!(!in_hang_window(9, 9000));
    assert!(!in_hang_window(8_947, 9000));
    assert!(in_hang_window(8_948, 9000));
    assert!(in_hang_window(9_000, 9000));
    assert!(in_hang_window(18_000, 9000));
    assert!(!in_hang_window(18_001 + 8, 9000));
    // W 16, TP2: DP 8 at stride 2 over 2 servers, 4 channels: 288,000 / 8 / 4 = 9,000 B.
    let trace = synthetic(&["r\t-1\t1\tNONE\t0\t1\tNONE\t0\t1\tREDUCESCATTER\t288000\t100"]);
    let error = try_plan(&trace, &options(Fidelity::Simai, "A100")).unwrap_err();
    assert!(error.contains("never marks sent"), "{error}");
    assert!(error.starts_with("line 3:"), "{error}");
    let plan = planned(&trace, &options(Fidelity::Megatron, "A100"));
    assert_eq!(plan.counters.hang_window_recorded, 1);
    // An all-to-all below one byte per pair: 1 B under SimAI (refused), elided under Megatron.
    let trace = synthetic(&["r\t-1\t1\tALLTOALL_EP\t7\t1\tNONE\t0\t1\tNONE\t0\t100"]);
    let error = try_plan(&trace, &options(Fidelity::Simai, "A100")).unwrap_err();
    assert!(error.contains("never marks sent"), "{error}");
    let plan = planned(&trace, &options(Fidelity::Megatron, "A100"));
    assert_eq!(
        (plan.ops.len(), plan.counters.elided_zero_all_to_all),
        (0, 1)
    );
    // A ring whose per-message floor is 0 sends nothing in SimAI.
    let trace = synthetic(&["r\t-1\t1\tNONE\t0\t1\tNONE\t0\t1\tREDUCESCATTER\t31\t100"]);
    let plan = planned(&trace, &options(Fidelity::Simai, "A100"));
    assert_eq!((plan.ops.len(), plan.counters.elided_ring_floor), (0, 1));
}

#[test]
fn simai_refusals() {
    let cases: Vec<(&str, PlanOptions, &str)> = vec![
        (
            "r\t-1\t1\tNONE\t0\t1\tALLGATHER\t0\t1\tNONE\t0\t100",
            options(Fidelity::Simai, "A100"),
            "InputGradient collective of 0 B",
        ),
        (
            "r\t-1\t2147483648\tNONE\t0\t1\tNONE\t0\t1\tNONE\t0\t100",
            options(Fidelity::Simai, "A100"),
            "narrows ticks to int",
        ),
        (
            "r\t-1\t1\tNONE\t0\t1\tNONE\t0\t1\tNONE\t0\t2147483648",
            options(Fidelity::Simai, "A100"),
            "process_time",
        ),
        (
            "r\t-1\t1\tALLGATHER\t4096\t1\tNONE\t0\t1\tNONE\t0\t100",
            PlanOptions {
                simai_env: None,
                ..options(Fidelity::Simai, "A100")
            },
            "needs the AS_SEND_LAT",
        ),
        (
            "r\t-1\t1\tALLGATHER\t4096\t1\tNONE\t0\t1\tNONE\t0\t100",
            PlanOptions {
                simai_env: Some(SimaiEnv {
                    pxn_enable: true,
                    ..SIMAI_ENV
                }),
                ..options(Fidelity::Simai, "A100")
            },
            "AS_PXN_ENABLE",
        ),
    ];
    for (record, options, expected) in cases {
        let error = try_plan(&synthetic(&[record]), &options).unwrap_err();
        assert!(error.contains(expected), "`{error}` lacks `{expected}`");
    }
    // The tick narrowing is a SimAI rule only.
    let trace = synthetic(&["r\t-1\t2147483648\tNONE\t0\t1\tNONE\t0\t1\tNONE\t0\t100"]);
    assert!(try_plan(&trace, &options(Fidelity::Megatron, "A100")).is_ok());
    // NVLS: b4's TP8 all-reduces on H100 with AS_NVLS_ENABLE = 1; fine on A100 or with it off.
    let b4 = fixture(B4);
    let error = try_plan(&b4, &options(Fidelity::Simai, "H100")).unwrap_err();
    assert!(error.contains("NVLS"), "{error}");
    assert!(try_plan(&b4, &options(Fidelity::Simai, "A100")).is_ok());
    let off = PlanOptions {
        simai_env: Some(SimaiEnv {
            nvls_enable: false,
            ..SIMAI_ENV
        }),
        ..options(Fidelity::Simai, "H100")
    };
    assert!(try_plan(&b4, &off).is_ok());
}

#[test]
fn the_data_queue_falls_back_to_fifo_when_the_lifo_test_fails() {
    // Backward order: fork r2, then r1's ig all-to-all (cross-host, unbounded) and fork r1, then
    // fork r0. The offset of r1's fork is unknown, so the order is FIFO, recorded as a fallback,
    // and the ordinals are a proxy.
    let trace = synthetic(&[
        "r0\t-1\t1\tNONE\t0\t1\tNONE\t0\t1\tREDUCESCATTER\t4000000\t100",
        "r1\t-1\t1\tNONE\t0\t1\tALLTOALL_EP\t800000\t1\tREDUCESCATTER\t4000000\t100",
        "r2\t-1\t1\tNONE\t0\t1\tNONE\t0\t1\tREDUCESCATTER\t4000000\t100",
    ]);
    let plan = planned(&trace, &options(Fidelity::Simai, "A100"));
    let queue = &plan.data_queue;
    assert_eq!(data_names(&trace, &plan, &queue.issued), ["r2", "r1", "r0"]);
    assert_eq!(queue.issue_offsets_ns, [Some(0), None, None]);
    assert_eq!(queue.kind, DataQueueOrder::FifoFallback);
    assert_eq!(queue.order, queue.issued);
    assert!(!plan.ecmp_ordinals_exact);
    // Without the all-to-all the three forks are 4 ns apart: LIFO.
    let trace = synthetic(&[
        "r0\t-1\t1\tNONE\t0\t1\tNONE\t0\t1\tREDUCESCATTER\t4000000\t100",
        "r1\t-1\t1\tNONE\t0\t1\tNONE\t0\t1\tREDUCESCATTER\t4000000\t100",
        "r2\t-1\t1\tNONE\t0\t1\tNONE\t0\t1\tREDUCESCATTER\t4000000\t100",
    ]);
    let plan = planned(&trace, &options(Fidelity::Simai, "A100"));
    assert_eq!(
        plan.data_queue.issue_offsets_ns,
        [Some(0), Some(4), Some(8)]
    );
    assert_eq!(plan.data_queue.kind, DataQueueOrder::Lifo);
    assert_eq!(
        data_names(&trace, &plan, &plan.data_queue.order),
        ["r2", "r0", "r1"]
    );
    assert!(plan.ecmp_ordinals_exact);
}

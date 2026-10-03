//! The DCQCN transition CSV of the pinned schema `days-gpu/plans/briefs/p16/dcqcn-schema.md`.

use days_executor::{
    DCQCN_ALPHA_ONE, DcqcnController, DcqcnControllerConfig, DcqcnTransitionKind,
    DcqcnTransitionRecord, EventKey, FlowId, MechanismTransitionRecord, NodeId,
    dcqcn_transitions_csv,
};

const HEADER: &str = "time_ns,event_phase,event_origin_node,event_origin_sequence,node_id,flow_id,kind,bound_ns,frozen,alpha_ticks,increase_fires,decrease_cuts,initial_rate_bps,minimum_rate_bps,maximum_rate_bps,additive_rate_bps,hyper_rate_bps,g_q63,alpha_interval_ns,decrease_interval_ns,increase_interval_ns,fast_recovery_steps,clamp_target_rate,before_alpha_q63,before_current_rate_bps,before_target_rate_bps,before_next_alpha_ns,before_next_decrease_ns,before_next_increase_ns,before_stage,before_armed,before_alpha_pending,before_decrease_pending,before_increase_armed,after_alpha_q63,after_current_rate_bps,after_target_rate_bps,after_next_alpha_ns,after_next_decrease_ns,after_next_increase_ns,after_stage,after_armed,after_alpha_pending,after_decrease_pending,after_increase_armed";

fn controller() -> DcqcnController {
    DcqcnController::new(DcqcnControllerConfig {
        initial_rate_bps: 10_000_000_000,
        minimum_rate_bps: 1_000_000_000,
        maximum_rate_bps: 20_000_000_000,
        additive_rate_bps: 500_000_000,
        hyper_rate_bps: 1_000_000_000,
        g_q63: DCQCN_ALPHA_ONE >> 8,
        alpha_interval_ns: 1_000,
        decrease_interval_ns: 4_000,
        increase_interval_ns: 300_000,
        fast_recovery_steps: 1,
        clamp_target_rate: false,
    })
    .unwrap()
}

fn record(sequence: u64, flow: u32) -> MechanismTransitionRecord {
    let before = controller();
    let mut after = before;
    let advance = after.on_feedback(7);
    MechanismTransitionRecord::Dcqcn(DcqcnTransitionRecord {
        key: EventKey {
            time_ns: 7,
            phase: 0,
            origin_node: NodeId(1),
            origin_seq: sequence,
        },
        node: NodeId(0),
        flow: FlowId(flow.into()),
        kind: DcqcnTransitionKind::Feedback,
        bound_ns: 7,
        advance,
        frozen: false,
        before,
        after,
    })
}

#[test]
fn the_dcqcn_certificate_is_stable_and_canonical() {
    let csv = dcqcn_transitions_csv(&[record(2, 0), record(1, 0)]).unwrap();
    let mut rows = csv.lines();
    assert_eq!(rows.next().unwrap(), HEADER);
    let header = HEADER.split(',').collect::<Vec<_>>();
    let column = |name: &str| header.iter().position(|column| *column == name).unwrap();
    let first = rows.next().unwrap().split(',').collect::<Vec<_>>();
    let second = rows.next().unwrap().split(',').collect::<Vec<_>>();
    assert_eq!(first.len(), header.len());
    assert_eq!(first[column("event_origin_sequence")], "1");
    assert_eq!(second[column("event_origin_sequence")], "2");
    assert_eq!(first[column("kind")], "feedback");
    assert_eq!(first[column("bound_ns")], "7");
    assert_eq!(first[column("g_q63")], (DCQCN_ALPHA_ONE >> 8).to_string());
    assert_eq!(first[column("before_armed")], "0");
    assert_eq!(first[column("after_armed")], "1");
    assert_eq!(first[column("after_next_alpha_ns")], "1007");
    assert_eq!(first[column("after_next_decrease_ns")], "4008");
    assert_eq!(first[column("after_decrease_pending")], "1");
    assert_eq!(
        first[column("after_alpha_q63")],
        DCQCN_ALPHA_ONE.to_string()
    );
}

/// One event can yield a row per flow (a host RESUME restarts several queue pairs), but never two
/// rows of one flow.
#[test]
fn rows_are_unique_per_event_and_flow() {
    let csv = dcqcn_transitions_csv(&[record(1, 1), record(1, 0)]).unwrap();
    let flows = csv
        .lines()
        .skip(1)
        .map(|row| row.split(',').nth(5).unwrap().to_owned())
        .collect::<Vec<_>>();
    assert_eq!(flows, ["0", "1"]);
    let error = dcqcn_transitions_csv(&[record(1, 0), record(1, 0)]).unwrap_err();
    assert_eq!(error.mechanism, "DCQCN");
    assert_eq!(error.duplicate_key.origin_seq, 1);
}

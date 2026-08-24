use days_executor::{
    DCQCN_FRACTION_SCALE, DcqcnController, DcqcnControllerConfig, DcqcnTransitionKind,
    DcqcnTransitionRecord, EventKey, FlowId, MechanismTransitionRecord, NodeId,
    dcqcn_transitions_csv,
};

fn controller() -> DcqcnController {
    DcqcnController::new(
        DcqcnControllerConfig {
            initial_rate_bps: 10_000_000_000,
            minimum_rate_bps: 1_000_000_000,
            maximum_rate_bps: 20_000_000_000,
            additive_rate_bps: 500_000_000,
            hyper_rate_bps: 1_000_000_000,
            g_ppb: DCQCN_FRACTION_SCALE / 2,
            decrease_ppb: DCQCN_FRACTION_SCALE / 2,
            cnp_interval_ns: 10_000,
            control_interval_ns: 100_000,
            increase_byte_threshold: 10_000,
        },
        100_000,
    )
    .unwrap()
}

fn record(sequence: u64) -> MechanismTransitionRecord {
    let before = controller();
    let mut after = before;
    assert!(after.on_cnp(0).unwrap());
    MechanismTransitionRecord::Dcqcn(DcqcnTransitionRecord {
        key: EventKey {
            time_ns: 0,
            phase: 0,
            origin_node: NodeId(1),
            origin_seq: sequence,
        },
        node: NodeId(0),
        flow: FlowId(0),
        kind: DcqcnTransitionKind::Cnp,
        applied: true,
        emitted_bytes: 0,
        before,
        after,
    })
}

#[test]
fn exact_dcqcn_certificate_is_stable_and_canonical() {
    let csv = dcqcn_transitions_csv(&[record(2), record(1)]).unwrap();
    let mut rows = csv.lines();
    let header = rows.next().unwrap();
    assert!(header.starts_with("time_ns,event_phase,event_origin_node,event_origin_sequence"));
    let first = rows.next().unwrap().split(',').collect::<Vec<_>>();
    let second = rows.next().unwrap().split(',').collect::<Vec<_>>();
    assert_eq!(first[3], "1");
    assert_eq!(second[3], "2");
    assert_eq!(first[6], "cnp");
    assert_eq!(first[7], "1");
    assert_eq!(first[19], "0");
    assert_eq!(first[28], "500000000");
    assert_eq!(first[29], "7500000000");
}

#[test]
fn duplicate_dcqcn_certificate_keys_are_rejected() {
    let error = dcqcn_transitions_csv(&[record(1), record(1)]).unwrap_err();
    assert_eq!(error.mechanism, "DCQCN");
    assert_eq!(error.duplicate_key.origin_seq, 1);
}

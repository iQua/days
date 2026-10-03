//! P16 D1 fix round 1 (the orchestrator's ruled addition to review finding 2): the CNP join for
//! unreliable DCQCN flows. A CNP that arrives at an unreliable flow's reaction point while its
//! controller is not frozen must apply as exactly one `feedback` row at the arrival's time; one
//! that arrives after the freeze (the flow's finishing or stopping tick, ruling D11) is ignored and
//! has none. `dcqcn_cnp_arrivals_csv` is the log LeanGuard's trace mode joins against
//! (`p10c_dcqcn_check trace`).

use std::collections::BTreeMap;
use std::path::Path;

use days::scenario::compile_config;
use days_executor::{
    DcqcnTransitionKind, MechanismTransitionRecord, ObservationMode, PacketKind, RunResult,
    dcqcn_cnp_arrivals_csv, run_scalar_with_observations,
};

fn run(name: &str) -> RunResult {
    let image = compile_config(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("configs")
            .join(name),
    )
    .unwrap_or_else(|error| panic!("{name} must lower: {error}"));
    run_scalar_with_observations(&image, None, ObservationMode::Full)
        .unwrap_or_else(|error| panic!("{name} must run: {error}"))
}

/// `(time, flow)` of every CNP arrival the CSV lists.
fn cnp_arrivals(result: &RunResult) -> Vec<(u64, u64)> {
    let csv = dcqcn_cnp_arrivals_csv(&result.arrivals, &result.observed_packets);
    let mut lines = csv.lines();
    assert_eq!(lines.next(), Some("time_ns,flow_id,payload"));
    lines
        .map(|line| {
            let fields = line.split(',').collect::<Vec<_>>();
            (fields[0].parse().unwrap(), fields[1].parse().unwrap())
        })
        .collect()
}

#[test]
fn every_cnp_arrival_before_its_flows_freeze_applies_as_one_feedback_row() {
    for (name, applied) in [
        ("p14/dcqcn_t26.toml", 2),
        ("p16/dcqcn_mlx_blocked.toml", 880),
        ("p16/dcqcn_mlx_coincident.toml", 42),
    ] {
        let result = run(name);
        let observed = result
            .observed_packets
            .iter()
            .filter(|packet| matches!(packet.kind, PacketKind::DcqcnCnp(_)))
            .count();
        let arrivals = cnp_arrivals(&result);
        assert!(
            !arrivals.is_empty() && arrivals.len() <= observed,
            "{name}: every listed arrival is an observed CNP"
        );
        let records = &result
            .diagnostics
            .as_ref()
            .expect("full")
            .mechanism_transitions;
        let mut freeze = BTreeMap::<u64, u64>::new();
        let mut feedback = BTreeMap::<(u64, u64), usize>::new();
        for record in records {
            if let MechanismTransitionRecord::Dcqcn(row) = record {
                if row.frozen {
                    freeze.insert(row.flow.0, row.key.time_ns);
                }
                if row.kind == DcqcnTransitionKind::Feedback {
                    *feedback.entry((row.key.time_ns, row.flow.0)).or_default() += 1;
                }
            }
        }
        let mut expected = BTreeMap::<(u64, u64), usize>::new();
        for &(time, flow) in &arrivals {
            // A CNP is a phase-0 arrival: at the freezing tick's own instant it still applies.
            if freeze.get(&flow).is_none_or(|&frozen| time <= frozen) {
                *expected.entry((time, flow)).or_default() += 1;
            }
        }
        assert_eq!(
            feedback, expected,
            "{name}: feedback rows are the CNP arrivals before the freeze"
        );
        assert_eq!(
            expected.values().sum::<usize>(),
            applied,
            "{name}: live CNPs"
        );
    }
}

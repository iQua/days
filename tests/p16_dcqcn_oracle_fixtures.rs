//! P16 D1 condition 1 (`days-gpu/plans/briefs/p16/dcqcn-go.md`): on every DCQCN and queue-pair
//! fixture, the lazy controller of the executor equals the test-only eager oracle
//! (`executor/tests/support/dcqcn_oracle.rs`: three timers, every alpha tick applied as it fires).
//!
//! The oracle sees only the feedback times the run recorded. Each DCQCN row of a flow is replayed in
//! `EventKey` order: the oracle fires its timers up to the row's bound, takes the row's feedback, and
//! must then equal the row's `after` state field for field at every feedback and at the freeze, and
//! in every rate-relevant field at every other row (alpha is lazy between feedbacks and cuts).

#[path = "../executor/tests/support/dcqcn_oracle.rs"]
mod oracle;

use std::collections::BTreeMap;
use std::path::Path;

use days::scenario::compile_config;
use days_executor::{
    DcqcnController, DcqcnTransitionKind, DcqcnTransitionRecord, FlowId, MechanismTransitionRecord,
    ObservationMode, run_scalar_with_observations,
};
use oracle::{EagerDcqcn, rate_state_matches};

/// Every DCQCN and queue-pair fixture of `configs/p14`, `configs/p15` and `configs/p16`, except
/// the 64-pair HPCC incast, which the release-only test below covers.
const FIXTURES: [&str; 31] = [
    "p14/dcqcn_10s_zero_xoff.toml",
    "p14/dcqcn_1s_zero_xoff.toml",
    "p14/dcqcn_2s_zero_xoff.toml",
    "p14/dcqcn_multi_zero_xoff.toml",
    "p14/dcqcn_simple_zero_xoff.toml",
    "p14/dcqcn_t26.toml",
    "p14/dcqcn_t26_pfc.toml",
    "p14/leanguard_dcqcn_zero_xoff.toml",
    "p15/hostpfc_bidir_drr.toml",
    "p15/hostpfc_bidir_wrr.toml",
    "p15/hostpfc_incast_lossless.toml",
    "p15/hostpfc_multi_qp_tcp.toml",
    "p15/roce_allgather_compute_lossy.toml",
    "p15/roce_allgather_lossless.toml",
    "p15/roce_cnp_under_pfc.toml",
    "p15/roce_compute_dag.toml",
    "p15/roce_feedback_priority.toml",
    "p15/roce_gbn_lossy.toml",
    "p15/roce_lossless_pfc.toml",
    "p15/roce_mixed_tcp.toml",
    "p15/roce_nack_only.toml",
    "p15/roce_ring_allreduce_lossless.toml",
    "p15/roce_ring_lossy.toml",
    "p15/roce_ring_release_paused.toml",
    "p15/roce_tcp_mixed_collectives.toml",
    "p15/roce_timeout.toml",
    "p16/dcqcn_mlx_coincident.toml",
    "p16/dcqcn_mlx_coincident_qp.toml",
    "p16/dcqcn_mlx_coincident_grid_qp.toml",
    "p16/dcqcn_mlx_coincident_pending.toml",
    "p16/dcqcn_mlx_window.toml",
];

#[derive(Debug, Default)]
struct Coverage {
    rows: usize,
    feedbacks: usize,
    freezes: usize,
    cuts: u64,
    increases: u64,
    /// Feedbacks at exactly an instant the eager oracle had pending.
    coincident_feedbacks: usize,
    /// Fix round 1 (review F1): the same-instant classes. A feedback at `t` (phase 0) precedes
    /// every timer at `t` (ruling D3), so each class pins one ordering: an alpha tick, a
    /// rate-increase (RP) fire, a pending decrease check, and an idle decrease-grid instant (the
    /// grid fires with nothing pending, so a feedback there opens a cut at `t`, not `t + D`).
    on_alpha_tick: usize,
    on_rp_fire: usize,
    on_pending_decrease: usize,
    on_idle_decrease_grid: usize,
}

impl Coverage {
    fn add(&mut self, other: &Coverage) {
        self.rows += other.rows;
        self.feedbacks += other.feedbacks;
        self.freezes += other.freezes;
        self.cuts += other.cuts;
        self.increases += other.increases;
        self.coincident_feedbacks += other.coincident_feedbacks;
        self.on_alpha_tick += other.on_alpha_tick;
        self.on_rp_fire += other.on_rp_fire;
        self.on_pending_decrease += other.on_pending_decrease;
        self.on_idle_decrease_grid += other.on_idle_decrease_grid;
    }
}

fn rows(name: &str) -> Vec<DcqcnTransitionRecord> {
    let image = compile_config(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("configs")
            .join(name),
    )
    .unwrap_or_else(|error| panic!("{name} must lower: {error}"));
    let result = run_scalar_with_observations(&image, None, ObservationMode::Full)
        .unwrap_or_else(|error| panic!("{name} must run: {error}"));
    result
        .diagnostics
        .expect("full observation")
        .mechanism_transitions
        .into_iter()
        .filter_map(|record| match record {
            MechanismTransitionRecord::Dcqcn(row) => Some(row),
            _ => None,
        })
        .collect()
}

fn replay(name: &str, coverage: &mut Coverage) {
    let mut flows = BTreeMap::<FlowId, Vec<DcqcnTransitionRecord>>::new();
    for row in rows(name) {
        flows.entry(row.flow).or_default().push(row);
    }
    for (flow, rows) in flows {
        let config = rows[0].before.config;
        let mut eager = EagerDcqcn::new(config);
        let mut expected_before = DcqcnController::pristine(config);
        for row in rows {
            let context = format!("{name} flow {flow:?} at {:?}", row.key);
            assert_eq!(row.before, expected_before, "{context}: continuity");
            coverage.rows += 1;
            coverage.cuts += row.advance.decrease_cuts;
            coverage.increases += row.advance.increase_fires;
            eager.advance_to(row.bound_ns);
            let eager_after = match row.kind {
                DcqcnTransitionKind::Feedback => {
                    let now = Some(row.key.time_ns);
                    let on_alpha = eager.next_alpha == now;
                    let on_rp = eager.increase_armed && eager.next_increase == now;
                    let on_check = eager.next_decrease == now;
                    coverage.coincident_feedbacks += usize::from(on_alpha || on_rp || on_check);
                    coverage.on_alpha_tick += usize::from(on_alpha);
                    coverage.on_rp_fire += usize::from(on_rp);
                    coverage.on_pending_decrease += usize::from(on_check && eager.decrease_pending);
                    coverage.on_idle_decrease_grid +=
                        usize::from(on_check && !eager.decrease_pending);
                    eager.feedback(row.key.time_ns);
                    coverage.feedbacks += 1;
                    eager.as_controller()
                }
                DcqcnTransitionKind::Tick | DcqcnTransitionKind::Advance => eager.as_controller(),
            };
            if row.kind == DcqcnTransitionKind::Feedback || row.frozen {
                coverage.freezes += usize::from(row.frozen);
                assert_eq!(row.after, eager_after, "{context}: the whole state");
            } else {
                assert!(
                    rate_state_matches(&row.after, &eager_after),
                    "{context}: rate state\nlazy  {:?}\neager {eager_after:?}",
                    row.after
                );
            }
            expected_before = row.after;
        }
    }
}

#[test]
fn the_lazy_controller_equals_the_eager_oracle_on_every_fixture() {
    let mut coverage = Coverage::default();
    for name in FIXTURES {
        replay(name, &mut coverage);
    }
    println!("record=oracle_fixtures {coverage:?}");
    assert!(coverage.feedbacks > 1_000, "{coverage:?}");
    assert!(coverage.freezes > 20, "{coverage:?}");
    assert!(
        coverage.cuts > 100 && coverage.increases > 100,
        "{coverage:?}"
    );
}

/// The synthetic images put feedback in the same nanosecond as alpha ticks, rate-increase fires and
/// decrease checks, pending and idle. Each must reach its controllers with feedback that cuts before
/// its flows finish (an unreliable flow's controller freezes when it has sent its last byte, ruling
/// D11), and together they must land feedback exactly on every class of instant (review F1): a
/// feedback at `t` precedes every timer at `t` (ruling D3), so each class pins one ordering.
#[test]
fn the_coincident_fixtures_exercise_same_instant_order() {
    let mut total = Coverage::default();
    for name in [
        "p16/dcqcn_mlx_coincident.toml",
        "p16/dcqcn_mlx_coincident_qp.toml",
        "p16/dcqcn_mlx_coincident_grid_qp.toml",
        "p16/dcqcn_mlx_coincident_pending.toml",
    ] {
        let mut coverage = Coverage::default();
        replay(name, &mut coverage);
        println!("record=oracle_coincident image={name} {coverage:?}");
        assert!(
            coverage.feedbacks > 0 && coverage.cuts > 0,
            "{name}: no live feedback {coverage:?}"
        );
        total.add(&coverage);
    }
    println!("record=oracle_coincident_total {total:?}");
    for (class, count) in [
        ("an alpha tick", total.on_alpha_tick),
        ("a rate-increase fire", total.on_rp_fire),
        ("a pending decrease check", total.on_pending_decrease),
        ("an idle decrease-grid instant", total.on_idle_decrease_grid),
    ] {
        assert!(count > 0, "no feedback lands exactly on {class}: {total:?}");
    }
}

#[test]
#[ignore = "release-only: 64 queue pairs on a 390-host Dragonfly embedding"]
fn the_lazy_controller_equals_the_eager_oracle_on_the_hpcc_incast() {
    let mut coverage = Coverage::default();
    replay("p15/hpcc_incast64_dragonfly.toml", &mut coverage);
    println!("record=oracle_hpcc {coverage:?}");
    assert!(coverage.feedbacks > 1_000, "{coverage:?}");
    assert_eq!(coverage.freezes, 64, "every pair freezes at completion");
}

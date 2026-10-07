//! P16 L2: Days AGO LeanGuard certificates from scenarios that lower through the Days AGO compiler.
//!
//! Each `configs/leanguard/<name>.toml` lowers through `compile_config`; the certificate its Scalar
//! full-observation run writes is committed under `lean/fixtures/`, byte for byte, and a CPU run
//! must write the same bytes. CI's LeanGuard job checks the committed certificates (expected
//! ACCEPT) and mutations of them (expected REJECT): `run-tcp-campaign.sh` and
//! `run-p10c-mechanism-campaign.sh` (its `wfq` mode certifies Days AGO's exact-rational WFQ), and
//! `run-sp-campaign.sh` for Static Priority. Set `DAYS_UPDATE_LEANGUARD_FIXTURES=1` to regenerate.

use std::fs;
use std::path::Path;

use days::scenario::compile_config;
use days_executor::{
    CpuConfig, DiagnosticPlanes, MechanismTransitionRecord, ObservationMode, RunResult,
    SimulationImage, drr_transitions_csv, pfc_transitions_csv, run_cpu_with_observations,
    run_scalar_with_observations, sp_transitions_csv, tcp_transitions_csv, wfq_transitions_csv,
    wrr_transitions_csv,
};

/// Which certificate family a fixture writes.
#[derive(Clone, Copy, Debug)]
enum Family {
    Tcp,
    Pfc,
    Drr,
    Wrr,
    Wfq,
    Sp,
}

impl Family {
    /// The certificate of a run of `image` (WFQ and SP certificates start from its queues).
    fn csv(self, diagnostics: &DiagnosticPlanes, image: &SimulationImage) -> String {
        let records = &diagnostics.mechanism_transitions;
        match self {
            Self::Tcp => tcp_transitions_csv(&diagnostics.tcp_transitions).unwrap(),
            Self::Pfc => pfc_transitions_csv(records).unwrap(),
            Self::Drr => drr_transitions_csv(records).unwrap(),
            Self::Wrr => wrr_transitions_csv(records).unwrap(),
            Self::Wfq => wfq_transitions_csv(records, image).unwrap(),
            Self::Sp => sp_transitions_csv(records, image).unwrap(),
        }
    }
}

fn lower(config: &str) -> SimulationImage {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("configs/leanguard")
        .join(format!("{config}.toml"));
    compile_config(&path).unwrap_or_else(|error| panic!("{} must lower: {error}", path.display()))
}

/// The certificate the Scalar oracle writes for `configs/leanguard/<config>.toml`, after checking
/// that a two-worker CPU run writes the same bytes.
fn certificate(config: &str, family: Family) -> String {
    certificates(config, &lower(config), &[family]).remove(0)
}

/// The certificates of `families` the Scalar oracle writes for `image`, after checking that a
/// two-worker CPU run writes the same bytes.
fn certificates(name: &str, image: &SimulationImage, families: &[Family]) -> Vec<String> {
    let scalar = run_scalar_with_observations(image, None, ObservationMode::Full).unwrap();
    let cpu = run_cpu_with_observations(
        image,
        None,
        CpuConfig {
            workers: 2,
            ..CpuConfig::default()
        },
        ObservationMode::Full,
    )
    .unwrap();
    families
        .iter()
        .map(|family| {
            let csv = family.csv(scalar.diagnostics.as_ref().unwrap(), image);
            assert_eq!(
                family.csv(cpu.result.diagnostics.as_ref().unwrap(), image),
                csv,
                "{name}: CPU and Scalar certificates differ"
            );
            csv
        })
        .collect()
}

/// The image that resumes `image` from its Scalar state at `horizon_ns` (the checkpoint of
/// `tests/p16_pfc_service_identity.rs`), and that state.
fn resumed(image: &SimulationImage, horizon_ns: u64) -> (SimulationImage, RunResult) {
    let checkpoint =
        run_scalar_with_observations(image, Some(horizon_ns), ObservationMode::Full).unwrap();
    let mut resumed = image.clone();
    resumed.host_states.clone_from(&checkpoint.host_states);
    resumed.switch_states.clone_from(&checkpoint.switch_states);
    resumed
        .initial_packets
        .clone_from(&checkpoint.resident_packets);
    resumed
        .initial_events
        .clone_from(&checkpoint.pending_events);
    (resumed, checkpoint)
}

/// Compares `csv` with the committed fixture, or writes it under `DAYS_UPDATE_LEANGUARD_FIXTURES`.
fn assert_fixture(csv: &str, fixture: &str) {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("lean/fixtures")
        .join(fixture);
    if std::env::var_os("DAYS_UPDATE_LEANGUARD_FIXTURES").is_some() {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, csv).unwrap();
    }
    assert_eq!(
        csv,
        fs::read_to_string(&path).unwrap_or_default(),
        "{fixture}"
    );
}

/// The number of data rows of `csv` whose `column` equals `value`.
fn rows_with(csv: &str, column: &str, value: &str) -> usize {
    let mut lines = csv.lines();
    let header = lines.next().unwrap();
    let index = header.split(',').position(|name| name == column).unwrap();
    lines
        .filter(|line| line.split(',').nth(index) == Some(value))
        .count()
}

#[test]
fn compiled_tcp_certificates_cover_loss_recovery_and_timeout() {
    for (config, fixture, algorithm) in [
        ("tcp_reno", "tcp/reno-compiled-tcp-events.csv", "Reno"),
        ("tcp_cubic", "tcp/cubic-compiled-tcp-events.csv", "CUBIC"),
    ] {
        let csv = certificate(config, Family::Tcp);
        assert_eq!(
            rows_with(&csv, "algorithm", algorithm),
            csv.lines().count() - 1
        );
        for kind in ["new_ack", "duplicate_ack", "timeout"] {
            assert!(
                rows_with(&csv, "kind", kind) > 0,
                "{config} has no {kind} row"
            );
        }
        assert!(
            rows_with(&csv, "after_phase", "fast_recovery") > 0,
            "{config} never enters fast recovery"
        );
        assert_fixture(&csv, fixture);
    }
}

#[test]
fn compiled_switch_pfc_certificate_pauses_and_resumes_a_switch_link() {
    let csv = certificate("pfc_switch", Family::Pfc);
    assert!(rows_with(&csv, "control_action", "pause") > 0);
    assert!(rows_with(&csv, "control_action", "resume") > 0);
    assert_fixture(&csv, "p10c/pfc_compiled_executor_accept.csv");
}

#[test]
fn compiled_round_robin_certificates_serve_both_classes() {
    for (config, family, fixture) in [
        (
            "sched_drr",
            Family::Drr,
            "p10c/drr_compiled_executor_accept.csv",
        ),
        (
            "sched_wrr",
            Family::Wrr,
            "p10c/wrr_compiled_executor_accept.csv",
        ),
    ] {
        let csv = certificate(config, family);
        // Flows 0 and 1 map to classes 0 and 1 (`flow % 2`); both must be served.
        assert!(csv.lines().count() > 1, "{config} wrote no rows");
        assert_fixture(&csv, fixture);
    }
}

#[test]
fn compiled_wfq_certificate_records_exact_tags_and_every_service_decision() {
    let csv = certificate("sched_wfq", Family::Wfq);
    let enqueues = rows_with(&csv, "kind", "enqueue");
    let selects = rows_with(&csv, "kind", "select");
    let completes = rows_with(&csv, "kind", "complete");
    assert!(enqueues > 0 && selects > 0 && completes > 0);
    assert_eq!(enqueues + selects + completes, csv.lines().count() - 1);
    // The weights [1, 3] make virtual time advance by fractions of a bit: the certificate carries
    // exact rationals, never a rounded projection.
    assert!(csv.lines().skip(1).any(|line| {
        line.split(',')
            .any(|field| field.contains('/') && !field.ends_with("/1"))
    }));
    assert_fixture(&csv, "p10c/wfq_compiled_executor_accept.csv");
}

/// `numerator/denominator` as a pair, compared by cross-multiplication.
fn ratio(value: &str) -> (u128, u128) {
    let (numerator, denominator) = value.split_once('/').unwrap();
    (numerator.parse().unwrap(), denominator.parse().unwrap())
}

fn less(left: (u128, u128), right: (u128, u128)) -> bool {
    left.0 * right.1 < right.0 * left.1
}

#[test]
fn compiled_wfq_certificate_under_pfc_serves_past_paused_smaller_tags() {
    let csv = certificate("wfq_pfc", Family::Wfq);
    let header = csv.lines().next().unwrap().split(',').collect::<Vec<_>>();
    let column = |name: &str| header.iter().position(|field| *field == name).unwrap();
    let (kind, finish, queued, paused) = (
        column("kind"),
        column("finish_tag"),
        column("queued_packets"),
        column("paused_priorities"),
    );
    // Some service start finds priority 3 paused and serves a packet whose finish tag exceeds a
    // paused packet's: the certificate records the decision that PFC changes.
    let bypasses = csv
        .lines()
        .skip(1)
        .map(|line| line.split(',').collect::<Vec<_>>())
        .filter(|row| row[kind] == "select" && row[paused] == "3")
        .filter(|row| {
            row[queued].split(';').any(|packet| {
                let fields = packet.split(':').collect::<Vec<_>>();
                fields[3] == "3" && less(ratio(fields[4]), ratio(row[finish]))
            })
        })
        .count();
    assert!(
        bypasses > 0,
        "no service start bypasses a paused smaller tag"
    );
    assert_fixture(&csv, "p10c/wfq_pfc_compiled_executor_accept.csv");
    // The pause log the WFQ checker joins its paused sets to.
    assert_fixture(
        &certificate("wfq_pfc", Family::Pfc),
        "p10c/wfq_pfc_compiled_executor_accept.pfc.csv",
    );
}

#[test]
fn compiled_sp_certificate_records_enqueue_schedule_and_depart() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("configs/leanguard/sched_sp.toml");
    let image = compile_config(&path).unwrap();
    // `sp_check` has no pause model: the SP certificate fixture runs without PFC.
    assert!(
        image
            .switch_states
            .iter()
            .flat_map(|state| &state.queues)
            .all(|queue| queue.pfc.is_none())
    );
    let csv = certificate("sched_sp", Family::Sp);
    for kind in ["enqueue", "schedule", "depart"] {
        assert!(rows_with(&csv, "kind", kind) > 0, "no {kind} row");
    }
    assert_fixture(&csv, "sp/sp_compiled_executor_accept.csv");
}

/// Runs resumed from a checkpoint start with non-empty WFQ and SP queues, a packet in service and
/// (WFQ) non-zero virtual time: their certificates open with `initial` rows carrying that state.
#[test]
fn resumed_wfq_and_sp_certificates_start_from_the_checkpoint_state() {
    for (config, family, fixture) in [
        (
            "sched_wfq",
            Family::Wfq,
            "p10c/wfq_resumed_executor_accept.csv",
        ),
        ("sched_sp", Family::Sp, "sp/sp_resumed_executor_accept.csv"),
    ] {
        let (image, checkpoint) = resumed(&lower(config), 3_000);
        assert!(
            checkpoint
                .switch_states
                .iter()
                .flat_map(|state| &state.queues)
                .any(|queue| !queue.queue.is_empty() && queue.in_service.is_some()),
            "{config}: the checkpoint holds no queued and in-service packets"
        );
        let csv = certificates(config, &image, &[family]).remove(0);
        assert!(csv.contains(",initial"), "{config}: no initial row");
        assert_fixture(&csv, fixture);
    }
}

/// The WFQ incast resumed from its seventh eighth-of-the-run checkpoint, taken while a switch
/// queue is paused: its WFQ certificate starts paused, and the PFC certificate of the same run is
/// the pause log the WFQ checker joins.
#[test]
fn resumed_wfq_incast_certificate_starts_paused() {
    let image = lower("wfq_incast");
    let (image, checkpoint) = resumed(&image, image.stop_time_ns / 8 * 7);
    assert!(
        checkpoint
            .switch_states
            .iter()
            .flat_map(|state| &state.queues)
            .filter_map(|queue| queue.pfc.as_ref())
            .any(|pfc| pfc.paused_by_controller.iter().any(|set| !set.is_empty())),
        "the checkpoint pauses no switch queue"
    );
    let mut csvs = certificates("wfq_incast", &image, &[Family::Wfq, Family::Pfc]);
    let pfc = csvs.pop().unwrap();
    let wfq = csvs.pop().unwrap();
    let header = wfq.lines().next().unwrap().split(',').collect::<Vec<_>>();
    let column = |name: &str| header.iter().position(|field| *field == name).unwrap();
    assert!(
        wfq.lines().skip(1).any(|line| {
            let row = line.split(',').collect::<Vec<_>>();
            row[column("kind")] == "initial" && !row[column("paused_priorities")].is_empty()
        }),
        "no initial row starts paused"
    );
    assert_fixture(&wfq, "p10c/wfq_incast_resumed_executor_accept.csv");
    assert_fixture(&pfc, "p10c/wfq_incast_resumed_executor_accept.pfc.csv");
}

#[test]
fn the_wfq_record_does_not_grow_the_mechanism_record() {
    // The WFQ record is boxed: full observation retains one `MechanismTransitionRecord` per
    // transition, and its size stays that of the largest variant before WFQ.
    assert_eq!(std::mem::size_of::<MechanismTransitionRecord>(), 384);
}

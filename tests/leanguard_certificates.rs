//! P16 L2: Days AGO LeanGuard certificates from scenarios that lower through the Days AGO compiler.
//!
//! Each `configs/leanguard/<name>.toml` lowers through `compile_config`; the certificate its Scalar
//! full-observation run writes is committed under `lean/fixtures/`, byte for byte, and a CPU run
//! must write the same bytes. CI's LeanGuard job checks the committed certificates (expected
//! ACCEPT) and mutations of them (expected REJECT): `run-tcp-campaign.sh` and
//! `run-p10c-mechanism-campaign.sh`. Set `DAYS_UPDATE_LEANGUARD_FIXTURES=1` to regenerate.

use std::fs;
use std::path::Path;

use days::scenario::compile_config;
use days_executor::{
    CpuConfig, DiagnosticPlanes, ObservationMode, drr_transitions_csv, pfc_transitions_csv,
    run_cpu_with_observations, run_scalar_with_observations, tcp_transitions_csv,
    wrr_transitions_csv,
};

/// Which certificate family a fixture writes.
#[derive(Clone, Copy, Debug)]
enum Family {
    Tcp,
    Pfc,
    Drr,
    Wrr,
}

impl Family {
    fn csv(self, diagnostics: &DiagnosticPlanes) -> String {
        let records = &diagnostics.mechanism_transitions;
        match self {
            Self::Tcp => tcp_transitions_csv(&diagnostics.tcp_transitions).unwrap(),
            Self::Pfc => pfc_transitions_csv(records).unwrap(),
            Self::Drr => drr_transitions_csv(records).unwrap(),
            Self::Wrr => wrr_transitions_csv(records).unwrap(),
        }
    }
}

/// The certificate the Scalar oracle writes for `configs/leanguard/<config>.toml`, after checking
/// that a two-worker CPU run writes the same bytes.
fn certificate(config: &str, family: Family) -> String {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("configs/leanguard")
        .join(format!("{config}.toml"));
    let image = compile_config(&path)
        .unwrap_or_else(|error| panic!("{} must lower: {error}", path.display()));
    let scalar = run_scalar_with_observations(&image, None, ObservationMode::Full).unwrap();
    let csv = family.csv(scalar.diagnostics.as_ref().unwrap());
    let cpu = run_cpu_with_observations(
        &image,
        None,
        CpuConfig {
            workers: 2,
            ..CpuConfig::default()
        },
        ObservationMode::Full,
    )
    .unwrap();
    assert_eq!(
        family.csv(cpu.result.diagnostics.as_ref().unwrap()),
        csv,
        "{config}: CPU and Scalar certificates differ"
    );
    csv
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
        assert_eq!(rows_with(&csv, "algorithm", algorithm), csv.lines().count() - 1);
        for kind in ["new_ack", "duplicate_ack", "timeout"] {
            assert!(rows_with(&csv, "kind", kind) > 0, "{config} has no {kind} row");
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

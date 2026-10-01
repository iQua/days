//! P14 T5: executor-generated collective progress certificates.
//!
//! Each fixture under `lean/fixtures/p10c/` is the exact Scalar trace of one scenario; the
//! LeanGuard campaign (`lean/scripts/run-p10c-collective-campaign.sh`) accepts them and rejects
//! their mutations. Set `DAYS_UPDATE_COLLECTIVE_TRACE_FIXTURES=1` to regenerate.

#[path = "collective_tcp.rs"]
#[allow(dead_code)]
mod tcp;

use std::fs;

use days_executor::{
    CollectiveActivationCause, CollectiveStageKind, ObservationMode, collective_transitions_csv,
    run_scalar_with_observations,
};

fn star_config(algorithm: &str, size: u64) -> String {
    tcp::tcp_collective_config(algorithm, 4, size, 100)
        .replace("duration = 0.05", "duration = 0.0001")
        .replace("low = 500, high = 500", "low = 3, high = 3")
}

fn lossy_ring_config() -> String {
    tcp::tcp_collective_config("RingAllReduce", 4, 20_000, 4)
        .replace("duration = 0.05", "duration = 5.0")
        .replace(
            "edges = [[0, 4], [1, 4], [2, 4], [3, 4]]",
            "edges = [[0, 4], [1, 4], [2, 5], [3, 5], [4, 5]]",
        )
        .replace("sources = [0, 1, 2, 3]", "sources = [0, 2, 1, 3]")
        .replace("sinks = [1, 2, 3, 0]", "sinks = [2, 1, 3, 0]")
}

fn compute_chain_config() -> String {
    tcp::tcp_collective_config("RingAllReduce", 3, 3_000, 100).replace(
        "[[collective]]\n",
        "[[collective]]\nname = \"grad\"\nafter = \"forward\"\n",
    ) + r#"
[[compute]]
name = "forward"
hosts = [0, 1, 2]
duration_ns = 5000

[[compute]]
name = "backward"
hosts = [0, 1, 2]
duration_ns = 7000
after = "grad"

[[compute]]
name = "optimizer"
hosts = [0, 1, 2]
duration_ns = 3000
after = "backward"
"#
}

#[test]
fn collective_certificates_are_scalar_generated() {
    let mut local_release = false;
    let mut inbound_release = false;
    for (fixture, config) in [
        (
            "collective_allgather_executor_accept.csv",
            star_config("AllGather", 10),
        ),
        (
            "collective_ring_allreduce_executor_accept.csv",
            star_config("RingAllReduce", 10),
        ),
        (
            "collective_ring_allreduce_lossy_executor_accept.csv",
            lossy_ring_config(),
        ),
        (
            "collective_compute_chain_executor_accept.csv",
            compute_chain_config(),
        ),
    ] {
        let image = tcp::compile_text("certificate", &config);
        let scalar = run_scalar_with_observations(&image, None, ObservationMode::Full).unwrap();
        let records = &scalar.diagnostics.as_ref().unwrap().mechanism_transitions;
        let rows = tcp::progress(&scalar);
        local_release |= rows
            .iter()
            .any(|row| row.cause == CollectiveActivationCause::LocalCompletion && row.activated);
        inbound_release |= rows
            .iter()
            .any(|row| row.cause == CollectiveActivationCause::InboundArrival && row.activated);
        if fixture.contains("compute") {
            assert!(
                rows.iter()
                    .any(|row| row.stage_kind == CollectiveStageKind::Compute)
            );
        }
        let csv = collective_transitions_csv(records).unwrap();
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("lean/fixtures/p10c")
            .join(fixture);
        if std::env::var_os("DAYS_UPDATE_COLLECTIVE_TRACE_FIXTURES").is_some() {
            fs::write(&path, &csv).unwrap();
        }
        assert_eq!(csv, fs::read_to_string(path).unwrap(), "{fixture}");
    }
    assert!(
        local_release && inbound_release,
        "the fixtures release stages both by local completion and by inbound delivery"
    );
}

use std::path::PathBuf;

use days::scenario::compile_config;
use days_executor::{CpuConfig, run_cpu, run_scalar};

fn fixture_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("configs/benchmarks/small_lookahead/open_loop_100g_1us_st.toml")
}

#[test]
fn exact_executor_matches_key_on_legacy_st_terminal_observations() {
    let image = compile_config(fixture_path()).unwrap();
    assert_eq!(
        image
            .channels
            .iter()
            .map(|channel| channel.min_delay_ns)
            .min(),
        Some(1080)
    );

    let scalar = run_scalar(&image, None).unwrap();
    let cpu = run_cpu(
        &image,
        None,
        CpuConfig {
            workers: 4,
            ..CpuConfig::default()
        },
    )
    .unwrap();
    assert_eq!(cpu.result, scalar);
    assert_eq!(cpu.rounds.len(), 79_203);

    // Harvested from a release Nexosim ST key-on run of this exact fixture:
    // sources.csv sent 99,000 packets / 99,000,000 bytes, sinks.csv received the same,
    // and switches.csv recorded zero drops.
    let summary = cpu.result.summary;
    assert_eq!(
        (
            summary.sourced_packets,
            summary.sourced_bytes,
            summary.received_packets,
            summary.received_bytes,
            summary.dropped_packets,
            summary.dropped_bytes,
        ),
        (99_000, 99_000_000, 99_000, 99_000_000, 0, 0)
    );
}

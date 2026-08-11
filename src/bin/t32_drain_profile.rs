//! Correctness-only runner for T32's exact drain counters and scalar root-group trace.
//!
//! This binary intentionally reports no clock or duration. Run one or more fixture paths in a
//! single process so the opt-in Metal diagnostic pipeline is compiled only once.

use std::fmt::{self, Debug, Write as _};
use std::path::{Path, PathBuf};

use days::scenario::compile_config;
#[cfg(any(
    feature = "cuda",
    all(feature = "metal-spike", target_vendor = "apple")
))]
use days_executor::DeviceCapacityCaps;
use days_executor::{DrainProfile, ObservationMode, RunResult};

const FNV1A64_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
const FNV1A64_PRIME: u64 = 0x0000_0100_0000_01b3;
#[cfg(any(
    feature = "cuda",
    all(feature = "metal-spike", target_vendor = "apple")
))]
const CAPACITY_CAPS: DeviceCapacityCaps = DeviceCapacityCaps {
    fallback_fel_events_per_lp: Some(16_384),
    queue_packets_per_lp: Some(2_048),
    channel_events_per_stream: Some(2_048),
    remote_staging_events_per_lp: Some(2_048),
    outbox_events_total: Some(2_000_000),
    tcp_receiver_ranges_per_flow: Some(64),
    tcp_ledger_segments_per_flow: Some(4_096),
    observation_events_per_lp: Some(512),
};

const FIXTURES: [(&str, &str, u64, u64, u64, u64); 7] = [
    (
        "e1_10",
        "configs/benchmarks/p12/e1_open_k32_load_10.toml",
        18,
        2_227_879,
        70_809_309,
        0x951c_ad2c_3d9f_39f8,
    ),
    (
        "e1_30",
        "configs/benchmarks/p12/e1_open_k32_load_30.toml",
        18,
        6_333_069,
        131_534_072,
        0x2dbb_d522_d243_3b86,
    ),
    (
        "e1_60",
        "configs/benchmarks/p12/e1_open_k32_load_60.toml",
        18,
        10_373_881,
        256_696_554,
        0xb1ba_5a9d_872d_abbc,
    ),
    (
        "e1_90",
        "configs/benchmarks/p12/e1_open_k32_load_90.toml",
        18,
        12_951_185,
        379_505_175,
        0x8ae1_9e3f_4c91_b029,
    ),
    (
        "k48",
        "configs/benchmarks/width_via_load_k48_h16/fattree_k48_h16_load_90_sustained.toml",
        1_002,
        4_245_398_171,
        2_227_821_985,
        0xc04b_51a5_7fc0_d763,
    ),
    (
        "frontier",
        "configs/benchmarks/p11/rq9_frontier_closed_k32.toml",
        1_151,
        674_774_349,
        2_274_074_943,
        0x475b_25a5_6369_d8f6,
    ),
    (
        "e5",
        "configs/benchmarks/p12/e5_wide_k32_q200.toml",
        664,
        212_378_014,
        50_572_617,
        0x56f7_b241_57e2_e852,
    ),
];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Fingerprint {
    bytes: u64,
    fnv1a64: u64,
}

struct FingerprintWriter(Fingerprint);

struct DeviceOutcome {
    result: RunResult,
    profile: DrainProfile,
    rounds: u64,
    transitions: u64,
}

impl fmt::Write for FingerprintWriter {
    fn write_str(&mut self, value: &str) -> fmt::Result {
        self.0.bytes = self
            .0
            .bytes
            .checked_add(value.len() as u64)
            .ok_or(fmt::Error)?;
        self.0.fnv1a64 = value.bytes().fold(self.0.fnv1a64, |hash, byte| {
            (hash ^ u64::from(byte)).wrapping_mul(FNV1A64_PRIME)
        });
        Ok(())
    }
}

fn fingerprint(value: &impl Debug) -> Fingerprint {
    let mut writer = FingerprintWriter(Fingerprint {
        bytes: 0,
        fnv1a64: FNV1A64_OFFSET_BASIS,
    });
    write!(&mut writer, "{value:#?}").expect("Debug fingerprint must fit in u64 bytes");
    writer.0
}

fn fixture_row(path: &Path) -> (&'static str, u64, u64, Fingerprint) {
    FIXTURES
        .iter()
        .find(|(_, candidate, ..)| path == Path::new(candidate))
        .map(|&(name, _, rounds, transitions, bytes, fnv1a64)| {
            (name, rounds, transitions, Fingerprint { bytes, fnv1a64 })
        })
        .unwrap_or_else(|| {
            panic!(
                "fixture is not one of T32's seven registered points: {}",
                path.display()
            )
        })
}

fn report_profile(name: &str, profile: &DrainProfile) {
    println!(
        "record=p12t32_drain_totals fixture={name} selected_events={} head_visits={} \
         remote_emissions={} binary_search_lookups={} binary_search_iterations={}",
        profile.selected_events,
        profile.head_visits,
        profile.remote_emissions,
        profile.binary_search_lookups,
        profile.binary_search_iterations,
    );
    for row in &profile.head_visit_histogram {
        println!(
            "record=p12t32_head_histogram fixture={name} heads_visited={} selected_events={}",
            row.heads_visited, row.selected_events,
        );
    }
    for row in &profile.outbound_degrees {
        println!(
            "record=p12t32_outbound_degree fixture={name} outbound_degree={} producers={} \
             remote_emissions={} binary_search_lookups={} binary_search_iterations={}",
            row.outbound_degree,
            row.producer_count,
            row.remote_emissions,
            row.binary_search_lookups,
            row.binary_search_iterations,
        );
    }
    for row in &profile.lookup_iteration_histogram {
        println!(
            "record=p12t32_lookup_histogram fixture={name} outbound_degree={} \
             binary_search_iterations={} lookups={}",
            row.outbound_degree, row.binary_search_iterations, row.lookups,
        );
    }
}

#[cfg(all(
    feature = "metal-spike",
    not(feature = "cuda"),
    target_vendor = "apple"
))]
fn run_device(path: &Path, image: &days_executor::SimulationImage) -> DeviceOutcome {
    use days_executor::{MetalConfig, MetalExecutor};

    let executor = MetalExecutor::new().expect("Metal executor must initialize");
    let config = MetalConfig {
        capacity_caps: CAPACITY_CAPS,
        max_capacity_retries: 16,
        ..MetalConfig::default()
    };
    let ordinary = executor
        .run_with_observations(image, None, config, ObservationMode::Summary)
        .unwrap_or_else(|error| panic!("{} production Metal failed: {error}", path.display()));
    let diagnostic = executor
        .run_drain_profile_with_observations(image, None, config, ObservationMode::Summary)
        .unwrap_or_else(|error| panic!("{} drain-profile Metal failed: {error}", path.display()));
    assert_eq!(diagnostic.run.result, ordinary.result);
    assert_eq!(diagnostic.run.rounds, ordinary.rounds);
    assert_eq!(diagnostic.run.transitions, ordinary.transitions);
    assert_eq!(
        diagnostic.run.capacity_retry_trace,
        ordinary.capacity_retry_trace
    );
    assert_eq!(
        diagnostic.run.continuation_relaunches,
        ordinary.continuation_relaunches
    );
    DeviceOutcome {
        result: ordinary.result,
        profile: diagnostic.profile,
        rounds: ordinary.rounds,
        transitions: ordinary.transitions,
    }
}

#[cfg(feature = "cuda")]
fn run_device(path: &Path, image: &days_executor::SimulationImage) -> DeviceOutcome {
    use days_executor::{CudaConfig, CudaExecutor};

    let executor = CudaExecutor::new().expect("CUDA executor must initialize");
    let config = CudaConfig {
        capacity_caps: CAPACITY_CAPS,
        max_capacity_retries: 16,
        ..CudaConfig::default()
    };
    let ordinary = executor
        .run_with_observations(image, None, config, ObservationMode::Summary)
        .unwrap_or_else(|error| panic!("{} production CUDA failed: {error}", path.display()));
    let diagnostic = executor
        .run_drain_profile_with_observations(image, None, config, ObservationMode::Summary)
        .unwrap_or_else(|error| panic!("{} drain-profile CUDA failed: {error}", path.display()));
    assert_eq!(diagnostic.run.result, ordinary.result);
    assert_eq!(diagnostic.run.rounds, ordinary.rounds);
    assert_eq!(diagnostic.run.transitions, ordinary.transitions);
    assert_eq!(
        diagnostic.run.capacity_retry_trace,
        ordinary.capacity_retry_trace
    );
    assert_eq!(
        diagnostic.run.continuation_relaunches,
        ordinary.continuation_relaunches
    );
    DeviceOutcome {
        result: ordinary.result,
        profile: diagnostic.profile,
        rounds: ordinary.rounds,
        transitions: ordinary.transitions,
    }
}

#[cfg(not(any(
    feature = "cuda",
    all(feature = "metal-spike", target_vendor = "apple")
)))]
fn run_device(_: &Path, _: &days_executor::SimulationImage) -> DeviceOutcome {
    panic!("t32_drain_profile requires --features metal-spike on Apple or --features cuda")
}

fn main() {
    let mut root_trace = false;
    let mut paths = Vec::new();
    for argument in std::env::args().skip(1) {
        if argument == "--root-trace" {
            root_trace = true;
        } else {
            paths.push(PathBuf::from(argument));
        }
    }
    if paths.is_empty() {
        paths.extend(FIXTURES.iter().map(|(_, path, ..)| PathBuf::from(path)));
    }

    for path in paths {
        let (name, expected_rounds, expected_transitions, expected_fingerprint) =
            fixture_row(&path);
        let image = compile_config(&path)
            .unwrap_or_else(|error| panic!("failed to lower {}: {error}", path.display()));
        let outcome = run_device(&path, &image);
        let actual = fingerprint(&outcome.result);
        assert_eq!(actual, expected_fingerprint, "{name} frozen identity moved");
        assert_eq!(outcome.rounds, expected_rounds);
        assert_eq!(outcome.transitions, expected_transitions);
        assert_eq!(
            outcome.profile.selected_events,
            u128::from(expected_transitions)
        );
        if name == "e5" {
            assert_eq!(
                outcome.profile.remote_emissions, outcome.result.summary.departed_packets,
                "E5 remote emissions must equal channel departures"
            );
            println!(
                "record=p12t32_e5_channel_reconciliation remote_emissions={} \
                 channel_departures={} equal=true",
                outcome.profile.remote_emissions, outcome.result.summary.departed_packets,
            );
        }
        println!(
            "record=p12t32_identity fixture={name} rounds={expected_rounds} \
             transitions={expected_transitions} result_bytes={} result_fnv1a64={:016x} \
             instrumentation_off_on_equal=true",
            actual.bytes, actual.fnv1a64,
        );
        report_profile(name, &outcome.profile);

        if root_trace {
            let scalar = days_executor::run_scalar_rounds_with_t32_root_trace(
                &image,
                None,
                ObservationMode::Summary,
            )
            .unwrap_or_else(|error| panic!("{name} scalar root trace failed: {error}"));
            assert_eq!(
                scalar.run.result, outcome.result,
                "{name} scalar/device result mismatch"
            );
            assert_eq!(scalar.root_group_trace.len(), scalar.run.rounds.len());
            let mut messages = 0_u128;
            for (round, (metrics, trace)) in scalar
                .run
                .rounds
                .iter()
                .zip(&scalar.root_group_trace)
                .enumerate()
            {
                messages = messages
                    .checked_add(u128::from(metrics.messages_exchanged))
                    .expect("message total must fit u128");
                assert!(trace.eligible_groups <= trace.publication_dirty_groups);
                assert!(trace.root_time_changed_groups <= trace.publication_dirty_groups);
                assert!(trace.receiver_only_groups <= trace.publication_dirty_groups);
                println!(
                    "record=p12t32_root_trace fixture={name} round={round} eligible_groups={} \
                     root_time_changed_groups={} publication_dirty_groups={} receiver_only_lps={} \
                     receiver_only_groups={}",
                    trace.eligible_groups,
                    trace.root_time_changed_groups,
                    trace.publication_dirty_groups,
                    trace.receiver_only_lps,
                    trace.receiver_only_groups,
                );
            }
            assert_eq!(messages, outcome.profile.remote_emissions);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{FIXTURES, Fingerprint, fixture_row};
    use std::path::Path;

    #[test]
    fn all_seven_registered_fixture_paths_have_frozen_identity_rows() {
        for &(name, path, rounds, transitions, bytes, fnv1a64) in &FIXTURES {
            assert_eq!(
                fixture_row(Path::new(path)),
                (name, rounds, transitions, Fingerprint { bytes, fnv1a64 })
            );
        }
    }

    #[test]
    fn counter_runner_rejects_paths_outside_the_frozen_roster() {
        assert!(
            std::panic::catch_unwind(|| {
                fixture_row(Path::new(
                    "other/configs/benchmarks/p12/e1_open_k32_load_10.toml",
                ))
            })
            .is_err()
        );
    }
}

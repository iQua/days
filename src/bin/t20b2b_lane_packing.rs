#![cfg_attr(
    not(any(
        all(feature = "metal-spike", target_vendor = "apple"),
        feature = "cuda"
    )),
    allow(dead_code)
)]

use std::fmt::Debug;

use days_executor::{
    DeviceCapacityCaps, DeviceLanePacking, LanePackingCounters, ObservationMode, RunResult,
};

const FNV1A64_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
const FNV1A64_PRIME: u64 = 0x00000100000001b3;

const fn sustained_capacity_caps() -> DeviceCapacityCaps {
    DeviceCapacityCaps {
        fallback_fel_events_per_lp: Some(16_384),
        queue_packets_per_lp: Some(2_048),
        channel_events_per_stream: Some(2_048),
        remote_staging_events_per_lp: Some(2_048),
        outbox_events_total: Some(2_000_000),
        tcp_receiver_ranges_per_flow: Some(64),
        tcp_ledger_segments_per_flow: Some(4_096),
        observation_events_per_lp: Some(512),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Fingerprint {
    bytes: usize,
    fnv1a64: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ResultFingerprints {
    state: Fingerprint,
    observed: Fingerprint,
    departures: Fingerprint,
    arrivals: Fingerprint,
}

fn fingerprint(value: &impl Debug) -> Fingerprint {
    let serialization = format!("{value:?}");
    Fingerprint {
        bytes: serialization.len(),
        fnv1a64: serialization
            .bytes()
            .fold(FNV1A64_OFFSET_BASIS, |hash, byte| {
                (hash ^ u64::from(byte)).wrapping_mul(FNV1A64_PRIME)
            }),
    }
}

fn result_fingerprints(result: &RunResult) -> ResultFingerprints {
    ResultFingerprints {
        state: fingerprint(&(
            &result.host_states,
            &result.switch_states,
            &result.summary,
            &result.resident_packets,
            &result.pending_events,
            &result.diagnostics,
        )),
        observed: fingerprint(&result.observed_packets),
        departures: fingerprint(&result.departures),
        arrivals: fingerprint(&result.arrivals),
    }
}

#[derive(Clone, Debug)]
struct Cli {
    fixture: String,
    arms: Vec<DeviceLanePacking>,
    samples: usize,
    observation_mode: ObservationMode,
    exclusive_horizon_ns: Option<u64>,
    profile_sort: bool,
    profile_only: bool,
    mechanism_only: bool,
}

impl Cli {
    fn parse() -> Self {
        let mut fixture = None;
        let mut arms = vec![
            DeviceLanePacking::Unpacked,
            DeviceLanePacking::Descending,
            DeviceLanePacking::Ascending,
        ];
        let mut samples = 3;
        let mut observation_mode = ObservationMode::Summary;
        let mut exclusive_horizon_ns = None;
        let mut profile_sort = false;
        let mut profile_only = false;
        let mut mechanism_only = false;
        let mut arguments = std::env::args().skip(1);
        while let Some(argument) = arguments.next() {
            match argument.as_str() {
                "--arms" => {
                    arms = arguments
                        .next()
                        .expect("--arms requires a value")
                        .split(',')
                        .map(parse_arm)
                        .collect();
                }
                "--samples" => {
                    samples = arguments
                        .next()
                        .expect("--samples requires a value")
                        .parse()
                        .expect("--samples must be an integer");
                }
                "--full" => observation_mode = ObservationMode::Full,
                "--exclusive-horizon-ns" => {
                    exclusive_horizon_ns = Some(
                        arguments
                            .next()
                            .expect("--exclusive-horizon-ns requires a value")
                            .parse()
                            .expect("--exclusive-horizon-ns must be an integer"),
                    );
                }
                "--profile-sort" => profile_sort = true,
                "--profile-only" => {
                    profile_sort = true;
                    profile_only = true;
                }
                "--mechanism-only" => mechanism_only = true,
                unknown if unknown.starts_with("--") => panic!("unknown argument {unknown}"),
                path if fixture.is_none() => fixture = Some(path.to_owned()),
                extra => panic!("unexpected second fixture path {extra}"),
            }
        }
        assert!(!arms.is_empty(), "--arms must include at least one arm");
        assert!(samples > 0, "--samples must be nonzero");
        #[cfg(not(feature = "lane-packing-counters"))]
        if mechanism_only {
            panic!("--mechanism-only requires --features lane-packing-counters");
        }
        #[cfg(feature = "lane-packing-counters")]
        if !mechanism_only {
            panic!(
                "lane-packing-counters builds are mechanism-only; use --mechanism-only or rebuild without the feature"
            );
        }
        Self {
            fixture: fixture.expect(
                "usage: t20b2b_lane_packing FIXTURE [--arms unpacked,descending,ascending] \
                 [--samples N] [--full] [--exclusive-horizon-ns N] [--profile-sort] \
                 [--profile-only] [--mechanism-only]",
            ),
            arms,
            samples,
            observation_mode,
            exclusive_horizon_ns,
            profile_sort,
            profile_only,
            mechanism_only,
        }
    }
}

fn parse_arm(value: &str) -> DeviceLanePacking {
    match value {
        "unpacked" => DeviceLanePacking::Unpacked,
        "descending" => DeviceLanePacking::Descending,
        "ascending" => DeviceLanePacking::Ascending,
        _ => panic!("unknown lane-packing arm {value}"),
    }
}

fn print_identity(
    backend: &str,
    fixture: &str,
    arm: DeviceLanePacking,
    sample: &str,
    observation_mode: ObservationMode,
    result: &RunResult,
) {
    let hashes = result_fingerprints(result);
    let measurement_planes = match observation_mode {
        ObservationMode::Full => "canonical_full",
        ObservationMode::Summary => "empty_by_mode",
    };
    println!(
        "record=t20b2b_identity backend={backend} fixture={fixture} packing={} sample={sample} \
         observation_mode={observation_mode:?} measurement_planes={measurement_planes} \
         equality=direct_RunResult_eq state_bytes={} state_fnv1a64={:016x} \
         observed_count={} observed_bytes={} observed_fnv1a64={:016x} \
         departures_count={} departures_bytes={} departures_fnv1a64={:016x} \
         arrivals_count={} arrivals_bytes={} arrivals_fnv1a64={:016x}",
        arm.label(),
        hashes.state.bytes,
        hashes.state.fnv1a64,
        result.observed_packets.len(),
        hashes.observed.bytes,
        hashes.observed.fnv1a64,
        result.departures.len(),
        hashes.departures.bytes,
        hashes.departures.fnv1a64,
        result.arrivals.len(),
        hashes.arrivals.bytes,
        hashes.arrivals.fnv1a64,
    );
}

fn print_mechanism(
    backend: &str,
    fixture: &str,
    arm: DeviceLanePacking,
    counters: &LanePackingCounters,
) {
    let mut maxima = counters.group_maxima();
    maxima.sort_unstable();
    let percentile = |numerator: usize, denominator: usize| -> u64 {
        if maxima.is_empty() {
            return 0;
        }
        let index = (maxima.len() - 1) * numerator / denominator;
        maxima[index]
    };
    let total = maxima.iter().map(|value| u128::from(*value)).sum::<u128>();
    let mean = if maxima.is_empty() {
        0.0
    } else {
        total as f64 / maxima.len() as f64
    };
    println!(
        "record=t20b2b_mechanism backend={backend} fixture={fixture} packing={} \
         counter_kind=integer_work_only timers=none rounds={} groups={} \
         lane_time_utilization={:.9} group_max_mean={mean:.6} group_max_p50={} \
         group_max_p90={} group_max_p99={} group_max_max={}",
        arm.label(),
        counters.rounds.len(),
        maxima.len(),
        counters.lane_time_utilization(),
        percentile(50, 100),
        percentile(90, 100),
        percentile(99, 100),
        maxima.last().copied().unwrap_or(0),
    );
}

#[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
mod app {
    use std::path::PathBuf;
    use std::time::Instant;

    use days::scenario::compile_config;
    use days_executor::{MetalConfig, MetalExecutor};

    use super::{Cli, print_identity, print_mechanism, sustained_capacity_caps};

    pub fn main() {
        let cli = Cli::parse();
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(&cli.fixture);
        let image = compile_config(&path)
            .unwrap_or_else(|error| panic!("failed to lower {}: {error}", path.display()));
        let executor = MetalExecutor::new().expect("Metal executor must initialize");
        println!(
            "record=t20b2b_protocol backend=metal fixture={} samples={} discarded_warmups_per_arm=1 \
             timing_build={} observation_mode={:?} exclusive_horizon_ns={:?} \
             identity=direct_RunResult_eq dispersion_rule=twice_abs_median_gap_lt_sum_of_ranges \
             capacity_caps={:?}",
            cli.fixture,
            cli.samples,
            if cfg!(feature = "lane-packing-counters") {
                "instrumented"
            } else {
                "production"
            },
            cli.observation_mode,
            cli.exclusive_horizon_ns,
            sustained_capacity_caps(),
        );

        let mut expected = None;
        for arm in cli.arms.iter().filter(|_| !cli.profile_only) {
            let run = executor
                .run_with_observations(
                    &image,
                    cli.exclusive_horizon_ns,
                    MetalConfig {
                        lane_packing: *arm,
                        capacity_caps: sustained_capacity_caps(),
                        ..MetalConfig::default()
                    },
                    cli.observation_mode,
                )
                .unwrap_or_else(|error| panic!("Metal {} warmup failed: {error}", arm.label()));
            check_result(&mut expected, &run.result, arm.label());
            print_identity(
                "metal",
                &cli.fixture,
                *arm,
                "warmup",
                cli.observation_mode,
                &run.result,
            );
            if cli.mechanism_only {
                print_mechanism(
                    "metal",
                    &cli.fixture,
                    *arm,
                    run.lane_packing_counters
                        .as_ref()
                        .expect("counter build must return counters"),
                );
            }
        }
        if cli.mechanism_only {
            return;
        }

        for sample in 0..if cli.profile_only { 0 } else { cli.samples } {
            for arm in cli
                .arms
                .iter()
                .cycle()
                .skip(sample % cli.arms.len())
                .take(cli.arms.len())
            {
                let started = Instant::now();
                let run = executor
                    .run_with_observations(
                        &image,
                        cli.exclusive_horizon_ns,
                        MetalConfig {
                            lane_packing: *arm,
                            capacity_caps: sustained_capacity_caps(),
                            ..MetalConfig::default()
                        },
                        cli.observation_mode,
                    )
                    .unwrap_or_else(|error| panic!("Metal {} sample failed: {error}", arm.label()));
                let api_ns = started.elapsed().as_nanos();
                check_result(&mut expected, &run.result, arm.label());
                println!(
                    "record=t20b2b_timing backend=metal fixture={} packing={} sample={} \
                     api_ns={api_ns} backend_wall_ns={} device_ns={} host_submit_ns={} \
                     encoded_attempts={} rounds={} transitions={} byte_identity=1",
                    cli.fixture,
                    arm.label(),
                    sample,
                    run.wall_ns,
                    run.device_ns,
                    run.host_encode_submit_ns,
                    run.encoded_attempts,
                    run.rounds,
                    run.transitions,
                );
                print_identity(
                    "metal",
                    &cli.fixture,
                    *arm,
                    &sample.to_string(),
                    cli.observation_mode,
                    &run.result,
                );
            }
        }

        if cli.profile_sort {
            for arm in &cli.arms {
                let run = executor
                    .run_with_observations_profiled(
                        &image,
                        cli.exclusive_horizon_ns,
                        MetalConfig {
                            lane_packing: *arm,
                            capacity_caps: sustained_capacity_caps(),
                            ..MetalConfig::default()
                        },
                        cli.observation_mode,
                    )
                    .unwrap_or_else(|error| {
                        panic!("Metal {} profile failed: {error}", arm.label())
                    });
                check_result(&mut expected, &run.result, arm.label());
                let profile = run
                    .phase_profile
                    .expect("profiled Metal run returns phases");
                let captured_useful_attempts =
                    profile.useful_attempts.min(profile.captured_attempts);
                println!(
                    "record=t20b2b_sort_profile backend=metal fixture={} packing={} \
                     total_useful_attempts={} captured_attempts={} captured_useful_attempts={} \
                     compaction_ns={} compaction_ns_per_captured_useful_attempt={} \
                     profile_timing_only=1 production_timing_arm=0 byte_identity=1",
                    cli.fixture,
                    arm.label(),
                    profile.useful_attempts,
                    profile.captured_attempts,
                    captured_useful_attempts,
                    profile.useful.compaction_ns,
                    profile.useful.compaction_ns / captured_useful_attempts.max(1),
                );
            }
        }
    }

    fn check_result(
        expected: &mut Option<days_executor::RunResult>,
        actual: &days_executor::RunResult,
        arm: &str,
    ) {
        if let Some(expected) = expected {
            assert_eq!(
                actual, expected,
                "Metal {arm} result must be byte-identical"
            );
        } else {
            *expected = Some(actual.clone());
        }
    }
}

#[cfg(all(
    feature = "cuda",
    not(all(feature = "metal-spike", target_vendor = "apple"))
))]
mod app {
    use std::path::PathBuf;
    use std::time::Instant;

    use days::scenario::compile_config;
    use days_executor::{CudaConfig, CudaExecutor};

    use super::{Cli, print_identity, print_mechanism, sustained_capacity_caps};

    pub fn main() {
        let cli = Cli::parse();
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(&cli.fixture);
        let image = compile_config(&path)
            .unwrap_or_else(|error| panic!("failed to lower {}: {error}", path.display()));
        let executor = CudaExecutor::new().expect("CUDA executor must initialize");
        println!(
            "record=t20b2b_protocol backend=cuda fixture={} samples={} discarded_warmups_per_arm=1 \
             timing_build={} observation_mode={:?} exclusive_horizon_ns={:?} \
             identity=direct_RunResult_eq dispersion_rule=twice_abs_median_gap_lt_sum_of_ranges \
             capacity_caps={:?}",
            cli.fixture,
            cli.samples,
            if cfg!(feature = "lane-packing-counters") {
                "instrumented"
            } else {
                "production"
            },
            cli.observation_mode,
            cli.exclusive_horizon_ns,
            sustained_capacity_caps(),
        );

        let mut expected = None;
        for arm in cli.arms.iter().filter(|_| !cli.profile_only) {
            let run = executor
                .run_with_observations(
                    &image,
                    cli.exclusive_horizon_ns,
                    CudaConfig {
                        lane_packing: *arm,
                        capacity_caps: sustained_capacity_caps(),
                        ..CudaConfig::default()
                    },
                    cli.observation_mode,
                )
                .unwrap_or_else(|error| panic!("CUDA {} warmup failed: {error}", arm.label()));
            check_result(&mut expected, &run.result, arm.label());
            print_identity(
                "cuda",
                &cli.fixture,
                *arm,
                "warmup",
                cli.observation_mode,
                &run.result,
            );
            if cli.mechanism_only {
                print_mechanism(
                    "cuda",
                    &cli.fixture,
                    *arm,
                    run.lane_packing_counters
                        .as_ref()
                        .expect("counter build must return counters"),
                );
            }
        }
        if cli.mechanism_only {
            return;
        }

        for sample in 0..if cli.profile_only { 0 } else { cli.samples } {
            for arm in cli
                .arms
                .iter()
                .cycle()
                .skip(sample % cli.arms.len())
                .take(cli.arms.len())
            {
                let started = Instant::now();
                let run = executor
                    .run_with_observations(
                        &image,
                        cli.exclusive_horizon_ns,
                        CudaConfig {
                            lane_packing: *arm,
                            capacity_caps: sustained_capacity_caps(),
                            ..CudaConfig::default()
                        },
                        cli.observation_mode,
                    )
                    .unwrap_or_else(|error| panic!("CUDA {} sample failed: {error}", arm.label()));
                let api_ns = started.elapsed().as_nanos();
                check_result(&mut expected, &run.result, arm.label());
                println!(
                    "record=t20b2b_timing backend=cuda fixture={} packing={} sample={} \
                     api_ns={api_ns} backend_wall_ns={} device_ns={} host_submit_ns={} \
                     graph_capture_ns={} graph_replays={} encoded_attempts={} rounds={} \
                     transitions={} byte_identity=1",
                    cli.fixture,
                    arm.label(),
                    sample,
                    run.wall_ns,
                    run.device_ns,
                    run.host_submit_ns,
                    run.graph_capture_ns,
                    run.graph_replays,
                    run.encoded_attempts,
                    run.rounds,
                    run.transitions,
                );
                print_identity(
                    "cuda",
                    &cli.fixture,
                    *arm,
                    &sample.to_string(),
                    cli.observation_mode,
                    &run.result,
                );
            }
        }

        if cli.profile_sort {
            for arm in &cli.arms {
                let profiled = executor
                    .run_profiled_with_observations(
                        &image,
                        cli.exclusive_horizon_ns,
                        CudaConfig {
                            lane_packing: *arm,
                            capacity_caps: sustained_capacity_caps(),
                            ..CudaConfig::default()
                        },
                        cli.observation_mode,
                    )
                    .unwrap_or_else(|error| panic!("CUDA {} profile failed: {error}", arm.label()));
                check_result(&mut expected, &profiled.run.result, arm.label());
                println!(
                    "record=t20b2b_sort_profile backend=cuda fixture={} packing={} \
                     recorded_attempts={} prepare_ns={} prepare_ns_per_recorded_attempt={} \
                     profile_timing_only=1 production_timing_arm=0 byte_identity=1",
                    cli.fixture,
                    arm.label(),
                    profiled.profile.recorded_attempts,
                    profiled.profile.prepare_ns,
                    profiled.profile.prepare_ns / profiled.profile.recorded_attempts.max(1),
                );
            }
        }
    }

    fn check_result(
        expected: &mut Option<days_executor::RunResult>,
        actual: &days_executor::RunResult,
        arm: &str,
    ) {
        if let Some(expected) = expected {
            assert_eq!(actual, expected, "CUDA {arm} result must be byte-identical");
        } else {
            *expected = Some(actual.clone());
        }
    }
}

#[cfg(any(
    all(feature = "metal-spike", target_vendor = "apple"),
    all(
        feature = "cuda",
        not(all(feature = "metal-spike", target_vendor = "apple"))
    )
))]
fn main() {
    app::main();
}

#[cfg(not(any(
    all(feature = "metal-spike", target_vendor = "apple"),
    all(
        feature = "cuda",
        not(all(feature = "metal-spike", target_vendor = "apple"))
    )
)))]
fn main() {
    panic!("t20b2b_lane_packing requires Metal on Apple or --features cuda");
}

#[cfg(test)]
mod tests {
    use super::{FNV1A64_OFFSET_BASIS, fingerprint, parse_arm};
    use days_executor::DeviceLanePacking;

    #[test]
    fn arm_names_are_explicit() {
        assert_eq!(parse_arm("unpacked"), DeviceLanePacking::Unpacked);
        assert_eq!(parse_arm("descending"), DeviceLanePacking::Descending);
        assert_eq!(parse_arm("ascending"), DeviceLanePacking::Ascending);
    }

    #[test]
    fn fingerprints_cover_debug_bytes() {
        let hash = fingerprint(&vec![1_u64, 2, 3]);
        assert_ne!(hash.fnv1a64, FNV1A64_OFFSET_BASIS);
        assert_eq!(hash.bytes, "[1, 2, 3]".len());
    }
}

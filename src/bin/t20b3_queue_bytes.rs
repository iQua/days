#![cfg_attr(
    not(any(
        all(feature = "metal-spike", target_vendor = "apple"),
        feature = "cuda"
    )),
    allow(dead_code)
)]

use std::fmt::Debug;
use std::path::PathBuf;
#[cfg(any(
    all(feature = "metal-spike", target_vendor = "apple"),
    feature = "cuda"
))]
use std::time::Instant;

use clap::{Parser, ValueEnum};
use days::scenario::compile_config;
#[cfg(any(
    all(feature = "metal-spike", target_vendor = "apple"),
    feature = "cuda"
))]
use days_executor::run_scalar_with_observations;
use days_executor::{
    DeviceCapacityCaps, DropMarkPolicy, EcnThresholdPolicy, ObservationMode, QueueDepthUnit,
    RunResult, SimulationImage,
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

fn capacity_caps(kind: RunKind) -> DeviceCapacityCaps {
    let mut caps = sustained_capacity_caps();
    if kind == RunKind::FullParity {
        caps.observation_events_per_lp = None;
    }
    caps
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum RunKind {
    Warmup,
    Sample,
    FullParity,
}

impl RunKind {
    const fn label(self) -> &'static str {
        match self {
            Self::Warmup => "warmup",
            Self::Sample => "sample",
            Self::FullParity => "full-parity",
        }
    }

    const fn observation_mode(self) -> ObservationMode {
        match self {
            Self::Warmup | Self::Sample => ObservationMode::Summary,
            Self::FullParity => ObservationMode::Full,
        }
    }
}

#[derive(Debug, Parser)]
#[command(about = "P11 T20b3 clean production queue-byte ablation harness")]
struct Cli {
    /// Scenario TOML, relative to the repository root or absolute.
    fixture: PathBuf,
    /// Execute one discarded warmup, one retained sample, or one Full parity run.
    #[arg(long, value_enum)]
    kind: RunKind,
    /// Stable record token supplied by the outer rotated-arm driver.
    #[arg(long)]
    sample_label: String,
    /// Packet size used to convert every TailDrop packet capacity to an exact byte capacity.
    #[arg(long)]
    policy_packet_bytes: u64,
    /// Optional exact per-LP FEL capacity override.
    #[arg(long)]
    max_fel_events_per_lp: Option<usize>,
    /// Capacity retry budget; zero proves that the selected sizing is single-shot sufficient.
    #[arg(long, default_value_t = 4)]
    max_capacity_retries: usize,
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct BytePolicyTransform {
    queues: usize,
    capacity_bytes: u64,
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
        )),
        observed: fingerprint(&result.observed_packets),
        departures: fingerprint(&result.departures),
        arrivals: fingerprint(&result.arrivals),
    }
}

fn apply_probe_byte_policy(
    image: &mut SimulationImage,
    policy_packet_bytes: u64,
) -> Result<BytePolicyTransform, String> {
    if policy_packet_bytes == 0 {
        return Err("--policy-packet-bytes must be nonzero".to_owned());
    }

    let mut queues = 0_usize;
    let mut capacity_bytes = None;
    for (switch_slot, state) in image.switch_states.iter().enumerate() {
        for (queue_slot, queue) in state.queues.iter().enumerate() {
            if queue.drop_mark != DropMarkPolicy::TailDrop {
                return Err(format!(
                    "switch state {switch_slot} queue {queue_slot} must start as TailDrop"
                ));
            }
            let queue_capacity_bytes = queue
                .queue_capacity_packets
                .checked_mul(policy_packet_bytes)
                .ok_or_else(|| {
                    format!(
                        "switch state {switch_slot} queue {queue_slot} byte capacity overflows u64"
                    )
                })?;
            if queue_capacity_bytes == 0 {
                return Err(format!(
                    "switch state {switch_slot} queue {queue_slot} must have nonzero capacity"
                ));
            }
            if let Some(expected) = capacity_bytes {
                if queue_capacity_bytes != expected {
                    return Err(format!(
                        "switch state {switch_slot} queue {queue_slot} has byte capacity \
                         {queue_capacity_bytes}, expected {expected}"
                    ));
                }
            } else {
                capacity_bytes = Some(queue_capacity_bytes);
            }
            queues += 1;
        }
    }
    let capacity_bytes = capacity_bytes.ok_or_else(|| "fixture has no switch queues".to_owned())?;

    for queue in image
        .switch_states
        .iter_mut()
        .flat_map(|state| &mut state.queues)
    {
        queue.drop_mark = DropMarkPolicy::EcnThreshold(EcnThresholdPolicy {
            unit: QueueDepthUnit::Bytes,
            capacity: capacity_bytes,
            threshold: capacity_bytes,
        });
    }

    Ok(BytePolicyTransform {
        queues,
        capacity_bytes,
    })
}

fn prepare(cli: &Cli) -> (SimulationImage, BytePolicyTransform) {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(&cli.fixture);
    let mut image = compile_config(&path)
        .unwrap_or_else(|error| panic!("failed to lower {}: {error}", path.display()));
    let transform = apply_probe_byte_policy(&mut image, cli.policy_packet_bytes)
        .unwrap_or_else(|error| panic!("failed to derive byte policy: {error}"));
    (image, transform)
}

fn print_protocol(backend: &str, cli: &Cli, transform: BytePolicyTransform) {
    println!(
        "record=p11_t20b3_protocol backend={backend} fixture={} kind={} sample_label={} \
         observation_mode={:?} source_policy=TailDrop source_unit=packets \
         policy=EcnThreshold policy_unit=bytes policy_packet_bytes={} queues={} \
         capacity_bytes={} threshold_bytes={} capacity_caps={:?} max_fel_events_per_lp={:?} \
         max_capacity_retries={} production_uninstrumented=1",
        cli.fixture.display(),
        cli.kind.label(),
        cli.sample_label,
        cli.kind.observation_mode(),
        cli.policy_packet_bytes,
        transform.queues,
        transform.capacity_bytes,
        transform.capacity_bytes,
        capacity_caps(cli.kind),
        cli.max_fel_events_per_lp,
        cli.max_capacity_retries,
    );
}

fn print_timing(
    backend: &str,
    cli: &Cli,
    api_ns: u128,
    backend_wall_ns: u64,
    device_ns: u64,
    rounds: u64,
    transitions: u64,
) {
    println!(
        "record=p11_t20b3_timing backend={backend} fixture={} kind={} sample_label={} \
         observation_mode=Summary api_ns={api_ns} backend_wall_ns={backend_wall_ns} \
         device_ns={device_ns} rounds={rounds} transitions={transitions} \
         production_uninstrumented=1",
        cli.fixture.display(),
        cli.kind.label(),
        cli.sample_label,
    );
}

fn print_identity(backend: &str, cli: &Cli, result: &RunResult, diagnostics: &str, equality: &str) {
    let hashes = result_fingerprints(result);
    println!(
        "record=p11_t20b3_identity backend={backend} fixture={} kind={} sample_label={} \
         observation_mode={:?} equality={equality} diagnostics={diagnostics} \
         state_bytes={} state_fnv1a64={:016x} observed_count={} observed_bytes={} \
         observed_fnv1a64={:016x} departures_count={} departures_bytes={} \
         departures_fnv1a64={:016x} arrivals_count={} arrivals_bytes={} \
         arrivals_fnv1a64={:016x}",
        cli.fixture.display(),
        cli.kind.label(),
        cli.sample_label,
        cli.kind.observation_mode(),
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

#[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
fn main() {
    use days_executor::{MetalConfig, MetalExecutor};

    let cli = Cli::parse();
    let (image, transform) = prepare(&cli);
    let executor = MetalExecutor::new().expect("Metal executor must initialize");
    print_protocol("metal", &cli, transform);
    let config = MetalConfig {
        capacity_caps: capacity_caps(cli.kind),
        max_fel_events_per_lp: cli.max_fel_events_per_lp,
        max_capacity_retries: cli.max_capacity_retries,
        ..MetalConfig::default()
    };

    match cli.kind {
        RunKind::Warmup | RunKind::Sample => {
            let started = Instant::now();
            let run = executor
                .run_with_observations(&image, None, config, ObservationMode::Summary)
                .expect("Metal production run must succeed");
            let api_ns = started.elapsed().as_nanos();
            print_timing(
                "metal",
                &cli,
                api_ns,
                run.wall_ns,
                run.device_ns,
                run.rounds,
                run.transitions,
            );
            print_identity("metal", &cli, &run.result, "N/A", "fingerprint");
        }
        RunKind::FullParity => {
            let mut expected = run_scalar_with_observations(&image, None, ObservationMode::Full)
                .expect("scalar Full oracle must succeed");
            expected.diagnostics = None;
            let run = executor
                .run_with_observations(&image, None, config, ObservationMode::Full)
                .expect("Metal Full run must succeed");
            assert!(
                run.result == expected,
                "Metal Full result must equal the normalized scalar oracle"
            );
            println!(
                "record=p11_t20b3_parity backend=metal fixture={} sample_label={} \
                 observation_mode=Full equality=direct_RunResult_eq",
                cli.fixture.display(),
                cli.sample_label,
            );
            print_identity(
                "metal",
                &cli,
                &run.result,
                "normalized_scalar_to_absent",
                "direct_RunResult_eq",
            );
        }
    }
}

#[cfg(all(
    feature = "cuda",
    not(all(feature = "metal-spike", target_vendor = "apple"))
))]
fn main() {
    use days_executor::{CudaConfig, CudaExecutor};

    let cli = Cli::parse();
    let (image, transform) = prepare(&cli);
    let executor = CudaExecutor::new().expect("CUDA executor must initialize");
    print_protocol("cuda", &cli, transform);
    let config = CudaConfig {
        capacity_caps: capacity_caps(cli.kind),
        max_fel_events_per_lp: cli.max_fel_events_per_lp,
        max_capacity_retries: cli.max_capacity_retries,
        ..CudaConfig::default()
    };

    match cli.kind {
        RunKind::Warmup | RunKind::Sample => {
            let started = Instant::now();
            let run = executor
                .run_with_observations(&image, None, config, ObservationMode::Summary)
                .expect("CUDA production run must succeed");
            let api_ns = started.elapsed().as_nanos();
            print_timing(
                "cuda",
                &cli,
                api_ns,
                run.wall_ns,
                run.device_ns,
                run.rounds,
                run.transitions,
            );
            print_identity("cuda", &cli, &run.result, "N/A", "fingerprint");
        }
        RunKind::FullParity => {
            let mut expected = run_scalar_with_observations(&image, None, ObservationMode::Full)
                .expect("scalar Full oracle must succeed");
            expected.diagnostics = None;
            let run = executor
                .run_with_observations(&image, None, config, ObservationMode::Full)
                .expect("CUDA Full run must succeed");
            assert!(
                run.result == expected,
                "CUDA Full result must equal the normalized scalar oracle"
            );
            println!(
                "record=p11_t20b3_parity backend=cuda fixture={} sample_label={} \
                 observation_mode=Full equality=direct_RunResult_eq",
                cli.fixture.display(),
                cli.sample_label,
            );
            print_identity(
                "cuda",
                &cli,
                &run.result,
                "normalized_scalar_to_absent",
                "direct_RunResult_eq",
            );
        }
    }
}

#[cfg(not(any(
    all(feature = "metal-spike", target_vendor = "apple"),
    feature = "cuda"
)))]
fn main() {
    panic!("t20b3_queue_bytes requires --features metal-spike or --features cuda")
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use days::scenario::compile_config;
    use days_executor::{DropMarkPolicy, QueueDepthUnit};

    use super::{
        FNV1A64_OFFSET_BASIS, RunKind, apply_probe_byte_policy, capacity_caps, fingerprint,
    };

    #[test]
    fn full_parity_derives_observation_capacity_without_changing_timing_caps() {
        assert_eq!(
            capacity_caps(RunKind::Sample).observation_events_per_lp,
            Some(512)
        );
        assert_eq!(
            capacity_caps(RunKind::FullParity).observation_events_per_lp,
            None
        );
    }

    #[test]
    fn k16_probe_policy_is_derived_exactly() {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("configs/benchmarks/p11/rq9_closed_k16.toml");
        let mut image = compile_config(&path).expect("K16 fixture must lower");
        let transform = apply_probe_byte_policy(&mut image, 1_460).expect("transform must succeed");

        assert_eq!(transform.queues, 5_120);
        assert_eq!(transform.capacity_bytes, 93_440);
        for queue in image.switch_states.iter().flat_map(|state| &state.queues) {
            let DropMarkPolicy::EcnThreshold(policy) = queue.drop_mark else {
                panic!("every switch queue must use the derived byte policy")
            };
            assert_eq!(policy.unit, QueueDepthUnit::Bytes);
            assert_eq!(policy.capacity, 93_440);
            assert_eq!(policy.threshold, 93_440);
        }
    }

    #[test]
    fn fingerprints_cover_the_exact_debug_bytes() {
        let hash = fingerprint(&vec![1_u64, 2, 3]);
        assert_eq!(hash.bytes, "[1, 2, 3]".len());
        assert_ne!(hash.fnv1a64, FNV1A64_OFFSET_BASIS);
        assert_eq!(hash, fingerprint(&vec![1_u64, 2, 3]));
    }
}

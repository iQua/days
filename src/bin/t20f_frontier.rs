use std::fmt::{self, Debug, Write as _};
use std::path::PathBuf;
use std::time::Instant;

use clap::{Parser, ValueEnum};
use days::scenario::compile_config;
use days_executor::{DeviceCapacityCaps, ObservationMode, RunResult, run_scalar_with_observations};

const FNV1A64_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
const FNV1A64_PRIME: u64 = 0x0000_0100_0000_01b3;

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

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum Engine {
    Scalar,
    Device,
}

#[derive(Debug, Parser)]
#[command(about = "Run the T20f frontier capability and emit a complete-state fingerprint")]
struct Cli {
    fixture: PathBuf,
    #[arg(long, value_enum)]
    engine: Engine,
    /// Optional exclusive endpoint for the shorter scalar/device identity probe.
    #[arg(long)]
    exclusive_horizon_ns: Option<u64>,
    /// Zero selects strict single-shot execution.
    #[arg(long, default_value_t = 4)]
    max_capacity_retries: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Fingerprint {
    bytes: u64,
    fnv1a64: u64,
}

struct FingerprintWriter(Fingerprint);

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
    write!(&mut writer, "{value:#?}").expect("debug serialization length must fit in u64");
    writer.0
}

fn print_result(engine: &str, result: &RunResult, lowering_ns: u128, run_ns: u128) {
    let fingerprint = fingerprint(result);
    println!(
        "record=p11_t20f_frontier_result engine={engine} lowering_ns={lowering_ns} \
         run_ns={run_ns} result_bytes={} result_fnv1a64={:016x} pending_events={} \
         resident_packets={} sourced_packets={} departed_packets={} received_packets={} \
         dropped_packets={}",
        fingerprint.bytes,
        fingerprint.fnv1a64,
        result.pending_events.len(),
        result.resident_packets.len(),
        result.summary.sourced_packets,
        result.summary.departed_packets,
        result.summary.received_packets,
        result.summary.dropped_packets,
    );
}

fn main() {
    let cli = Cli::parse();
    let lowering_started = Instant::now();
    let image = compile_config(&cli.fixture)
        .unwrap_or_else(|error| panic!("failed to lower {}: {error}", cli.fixture.display()));
    let lowering_ns = lowering_started.elapsed().as_nanos();
    println!(
        "record=p11_t20f_frontier_protocol fixture={} engine={:?} \
         exclusive_horizon_ns={:?} observation_mode=Summary capacity_caps={CAPACITY_CAPS:?} \
         max_capacity_retries={}",
        cli.fixture.display(),
        cli.engine,
        cli.exclusive_horizon_ns,
        cli.max_capacity_retries,
    );

    match cli.engine {
        Engine::Scalar => {
            let started = Instant::now();
            let result = run_scalar_with_observations(
                &image,
                cli.exclusive_horizon_ns,
                ObservationMode::Summary,
            )
            .expect("scalar frontier run must succeed");
            print_result("scalar", &result, lowering_ns, started.elapsed().as_nanos());
        }
        Engine::Device => run_device(&cli, &image, lowering_ns),
    }
}

#[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
fn run_device(cli: &Cli, image: &days_executor::SimulationImage, lowering_ns: u128) {
    use days_executor::{MetalConfig, MetalExecutor};

    let executor = MetalExecutor::new().expect("Metal executor must initialize");
    let started = Instant::now();
    let run = executor
        .run_with_observations(
            image,
            cli.exclusive_horizon_ns,
            MetalConfig {
                capacity_caps: CAPACITY_CAPS,
                max_capacity_retries: cli.max_capacity_retries,
                ..MetalConfig::default()
            },
            ObservationMode::Summary,
        )
        .expect("Metal frontier run must succeed");
    let run_ns = started.elapsed().as_nanos();
    println!(
        "record=p11_t20f_frontier_device engine=metal wall_ns={} device_ns={} rounds={} \
         transitions={} retry_trace={:?}",
        run.wall_ns, run.device_ns, run.rounds, run.transitions, run.capacity_retry_trace,
    );
    print_result("metal", &run.result, lowering_ns, run_ns);
}

#[cfg(all(
    feature = "cuda",
    not(all(feature = "metal-spike", target_vendor = "apple"))
))]
fn run_device(cli: &Cli, image: &days_executor::SimulationImage, lowering_ns: u128) {
    use days_executor::{CudaConfig, CudaExecutor};

    let executor = CudaExecutor::new().expect("CUDA executor must initialize");
    let started = Instant::now();
    let run = executor
        .run_with_observations(
            image,
            cli.exclusive_horizon_ns,
            CudaConfig {
                capacity_caps: CAPACITY_CAPS,
                max_capacity_retries: cli.max_capacity_retries,
                ..CudaConfig::default()
            },
            ObservationMode::Summary,
        )
        .expect("CUDA frontier run must succeed");
    let run_ns = started.elapsed().as_nanos();
    println!(
        "record=p11_t20f_frontier_device engine=cuda wall_ns={} device_ns={} rounds={} \
         transitions={} retry_trace={:?}",
        run.wall_ns, run.device_ns, run.rounds, run.transitions, run.capacity_retry_trace,
    );
    print_result("cuda", &run.result, lowering_ns, run_ns);
}

#[cfg(not(any(
    all(feature = "metal-spike", target_vendor = "apple"),
    all(
        feature = "cuda",
        not(all(feature = "metal-spike", target_vendor = "apple"))
    )
)))]
fn run_device(_cli: &Cli, _image: &days_executor::SimulationImage, _lowering_ns: u128) {
    panic!("device mode requires --features metal-spike on Apple or --features cuda")
}

#[cfg(test)]
mod tests {
    use super::fingerprint;

    #[test]
    fn streaming_fingerprint_is_deterministic() {
        let value = vec![1_u64, 2, 3];
        assert_eq!(fingerprint(&value), fingerprint(&value));
        assert_ne!(fingerprint(&value), fingerprint(&vec![3_u64, 2, 1]));
    }
}

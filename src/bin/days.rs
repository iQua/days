//! `days`: compile one scenario and run it on one engine.
//!
//! `--engine scalar` runs the global Scalar oracle, `--engine cpu` the round-structured CPU
//! executor (`--workers 1` is its serial path), and `--engine metal` / `--engine cuda` the device
//! executors. Every engine ends with a `record=days_result` line carrying the complete-state
//! fingerprint, so results from different engines compare directly: equal `result_bytes` and
//! `result_fnv1a64` mean byte-identical complete state.
//!
//! A device engine that this binary was not built for is an explicit error naming the missing
//! feature; the engine is never chosen automatically.

use std::fmt::{self, Debug, Write as _};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::str::FromStr;
use std::time::Instant;

use clap::{Parser, ValueEnum};
use days::scenario::compile_config;
#[cfg(any(
    test,
    all(feature = "metal", target_vendor = "apple"),
    feature = "cuda"
))]
use days_executor::CapacityRetryRecord;
use days_executor::{
    ChunkGranularity, CpuConfig, CpuRoundMetrics, DeviceCapacityCaps, RoundMetrics, RunResult,
    SimulationImage, StaticPartitionPolicy, run_cpu, run_scalar,
};

const FNV1A64_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
const FNV1A64_PRIME: u64 = 0x0000_0100_0000_01b3;

/// Stock device capacity caps; `--channel-events-per-stream` overrides one lane.
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

const DEFAULT_MAX_CAPACITY_RETRIES: usize = 16;
const DEFAULT_ROUND_THREADS_PER_BLOCK: usize = 256;

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum Engine {
    Scalar,
    Cpu,
    Metal,
    Cuda,
}

/// `--chunk static|N`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Chunk(ChunkGranularity);

impl FromStr for Chunk {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value == "static" {
            return Ok(Self(ChunkGranularity::Static));
        }
        value
            .parse()
            .map(|size| Self(ChunkGranularity::Fixed(size)))
            .map_err(|error| format!("expected `static` or a chunk size: {error}"))
    }
}

/// `--straggler-threshold none|N`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct StragglerThreshold(Option<u64>);

impl FromStr for StragglerThreshold {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value == "none" {
            return Ok(Self(None));
        }
        value
            .parse()
            .map(|events| Self(Some(events)))
            .map_err(|error| format!("expected `none` or an event count: {error}"))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum StaticPartition {
    Modulo,
    RouteLoad,
}

#[derive(Debug, Parser)]
#[command(
    name = "days",
    about = "Compile one Days scenario and run it on one engine",
    long_about = None
)]
struct Cli {
    /// Scenario TOML.
    config: PathBuf,
    /// Engine to run: the Scalar oracle, the CPU executor, or a device executor.
    #[arg(long, value_enum)]
    engine: Engine,
    /// Optional exclusive endpoint for a shorter partial run (every engine).
    #[arg(long)]
    exclusive_horizon_ns: Option<u64>,

    /// CPU: worker threads (N >= 1; `--workers 1` is the serial round path). Required.
    #[arg(long, help_heading = "CPU engine")]
    workers: Option<usize>,
    /// CPU: bulk chunking, `static` or a fixed chunk size.
    #[arg(long, help_heading = "CPU engine")]
    chunk: Option<Chunk>,
    /// CPU: straggler threshold in events, or `none`.
    #[arg(long, help_heading = "CPU engine")]
    straggler_threshold: Option<StragglerThreshold>,
    /// CPU: workers dedicated to stragglers.
    #[arg(long, help_heading = "CPU engine")]
    dedicated: Option<usize>,
    /// CPU: spin iterations before a worker parks.
    #[arg(long, help_heading = "CPU engine")]
    spin_before_park: Option<u32>,
    /// CPU: static partition policy.
    #[arg(long, value_enum, help_heading = "CPU engine")]
    static_partition: Option<StaticPartition>,
    /// CPU: number of repetitions, each printing one record pair.
    #[arg(long, help_heading = "CPU engine")]
    repetitions: Option<usize>,

    /// Device: capacity retry budget; zero selects strict single-shot execution. Default 16.
    #[arg(long, help_heading = "Metal and CUDA engines")]
    max_capacity_retries: Option<usize>,
    /// Device: plan the first attempt from a snapshot written by `--dump-capacity-warm-start` for
    /// the same fixture.
    ///
    /// This is a host sizing hint, not an image or kernel cache: device capacity is refuse-or-run
    /// and never semantics, so the complete-state fingerprint is identical with and without it.
    #[arg(long, help_heading = "Metal and CUDA engines")]
    capacity_warm_start: Option<PathBuf>,
    /// Device: write the capacity this run converged on, for a later `--capacity-warm-start`.
    #[arg(long, help_heading = "Metal and CUDA engines")]
    dump_capacity_warm_start: Option<PathBuf>,
    /// Device: override the plane-wide channel-stream starting cap.
    #[arg(long, help_heading = "Metal and CUDA engines")]
    channel_events_per_stream: Option<usize>,
    /// Device: override the round/exchange launch width. Default 256.
    #[arg(long, help_heading = "Metal and CUDA engines")]
    round_threads_per_block: Option<usize>,
}

impl Cli {
    fn cpu_options(&self) -> [(&'static str, bool); 7] {
        [
            ("--workers", self.workers.is_some()),
            ("--chunk", self.chunk.is_some()),
            ("--straggler-threshold", self.straggler_threshold.is_some()),
            ("--dedicated", self.dedicated.is_some()),
            ("--spin-before-park", self.spin_before_park.is_some()),
            ("--static-partition", self.static_partition.is_some()),
            ("--repetitions", self.repetitions.is_some()),
        ]
    }

    fn device_options(&self) -> [(&'static str, bool); 5] {
        [
            (
                "--max-capacity-retries",
                self.max_capacity_retries.is_some(),
            ),
            ("--capacity-warm-start", self.capacity_warm_start.is_some()),
            (
                "--dump-capacity-warm-start",
                self.dump_capacity_warm_start.is_some(),
            ),
            (
                "--channel-events-per-stream",
                self.channel_events_per_stream.is_some(),
            ),
            (
                "--round-threads-per-block",
                self.round_threads_per_block.is_some(),
            ),
        ]
    }

    /// Refuses options that do not apply to the selected engine, rather than ignoring them.
    fn check_options(&self) -> Result<(), String> {
        let refuse = |options: &[(&'static str, bool)], family: &str| match options
            .iter()
            .find(|(_, given)| *given)
        {
            Some((flag, _)) => Err(format!(
                "{flag} is a {family} option and does not apply to --engine {}",
                self.engine_name()
            )),
            None => Ok(()),
        };
        match self.engine {
            Engine::Scalar => {
                refuse(&self.cpu_options(), "CPU-engine")?;
                refuse(&self.device_options(), "device-engine")
            }
            Engine::Cpu => {
                refuse(&self.device_options(), "device-engine")?;
                match self.workers {
                    None => Err("--engine cpu requires --workers N (N >= 1)".to_owned()),
                    Some(0) => Err("--workers must be at least 1".to_owned()),
                    Some(_) => Ok(()),
                }
            }
            Engine::Metal | Engine::Cuda => refuse(&self.cpu_options(), "CPU-engine"),
        }
    }

    fn engine_name(&self) -> &'static str {
        match self.engine {
            Engine::Scalar => "scalar",
            Engine::Cpu => "cpu",
            Engine::Metal => "metal",
            Engine::Cuda => "cuda",
        }
    }

    fn cpu_config(&self) -> CpuConfig {
        let default = CpuConfig::default();
        CpuConfig {
            workers: self.workers.unwrap_or(default.workers),
            granularity: self.chunk.map_or(ChunkGranularity::Static, |chunk| chunk.0),
            static_partition: match self.static_partition {
                None => default.static_partition,
                Some(StaticPartition::Modulo) => StaticPartitionPolicy::Modulo,
                Some(StaticPartition::RouteLoad) => StaticPartitionPolicy::RouteLoad,
            },
            straggler_threshold_events: self.straggler_threshold.and_then(|threshold| threshold.0),
            dedicated_straggler_workers: self.dedicated.unwrap_or(1),
            spin_before_park: self.spin_before_park.unwrap_or(default.spin_before_park),
            ..default
        }
    }

    fn max_capacity_retries(&self) -> usize {
        self.max_capacity_retries
            .unwrap_or(DEFAULT_MAX_CAPACITY_RETRIES)
    }
}

fn effective_caps(cli: &Cli) -> DeviceCapacityCaps {
    let mut caps = CAPACITY_CAPS;
    if let Some(cap) = cli.channel_events_per_stream {
        caps.channel_events_per_stream = Some(cap);
    }
    caps
}

fn effective_round_threads(cli: &Cli, default: usize) -> usize {
    cli.round_threads_per_block.unwrap_or(default)
}

/// The `--capacity-warm-start` / `--dump-capacity-warm-start` snapshot file format.
///
/// The executor owns the semantics; this module owns exactly one canonical textual form for them,
/// so a snapshot round-trips and two runs of the same fixture write the same bytes.
#[cfg(any(
    test,
    all(feature = "metal", target_vendor = "apple"),
    feature = "cuda"
))]
mod warm_start {
    use days_executor::{CapacityWarmStart, DeviceCapacityFloors};

    /// The eleven plane-wide floor lanes, in the fixed order the format writes them.
    pub(super) const FLOOR_LANES: [&str; 11] = [
        "fallback_fel_events_per_lp",
        "queue_packets_per_lp",
        "channel_events_per_stream",
        "service_events_per_stream",
        "generator_events_per_stream",
        "remote_staging_events_per_lp",
        "outbox_events_total",
        "tcp_receiver_ranges_per_flow",
        "tcp_ledger_segments_per_flow",
        "observation_events",
        "worklist_entries_total",
    ];

    /// Format version.
    ///
    /// `v1` had no terminator, so a truncated snapshot parsed as a silently downgraded partial
    /// hint, and no capacity ceiling, so an in-format magnitude aborted the allocator inside the
    /// planner. Both are fixed by shape changes to the file, so the version is bumped rather than
    /// mutated in place: every `v1` snapshot on disk is refused by its header, which is the
    /// correct outcome for a file that cannot be validated.
    pub(super) const HEADER: &str = "days-capacity-warm-start v2";

    /// Terminator keyword: the number of association records the snapshot carries.
    ///
    /// The three association lists have no declared length, and the floor block is written first,
    /// so without this a snapshot truncated anywhere after the floors parses *successfully* as a
    /// partial hint — exactly the silent downgrade this format claims to refuse.
    const RECORD_COUNT: &str = "records";

    /// The largest capacity, in records, a snapshot file may name.
    ///
    /// A capacity is a record count that the planner multiplies by a record width — the narrowest
    /// is `TCP_RANGE_WORDS = 2` u64 words — and sums into one plan. At this ceiling a **single**
    /// entity already claims `2 * 8 * 2^32 = 68.7 GB`, more than any device in the fleet. No value
    /// at or above it can be a capacity any device attempt converged on, so a file naming one is
    /// corrupt and is refused **here**, before anything is allocated, instead of as an allocator
    /// abort inside the planner.
    ///
    /// This bounds the **file**, which is the untrusted surface this flag adds. A
    /// `CapacityWarmStart` constructed in process has exactly the standing `DeviceCapacityFloors`
    /// and the `max_*` overrides already have — its magnitudes are the caller's responsibility.
    const MAX_CAPACITY: u64 = 1 << 32;

    fn floor_values(floors: DeviceCapacityFloors) -> [usize; FLOOR_LANES.len()] {
        [
            floors.fallback_fel_events_per_lp,
            floors.queue_packets_per_lp,
            floors.channel_events_per_stream,
            floors.service_events_per_stream,
            floors.generator_events_per_stream,
            floors.remote_staging_events_per_lp,
            floors.outbox_events_total,
            floors.tcp_receiver_ranges_per_flow,
            floors.tcp_ledger_segments_per_flow,
            floors.observation_events,
            floors.worklist_entries_total,
        ]
    }

    fn floors_from(values: [usize; FLOOR_LANES.len()]) -> DeviceCapacityFloors {
        DeviceCapacityFloors {
            fallback_fel_events_per_lp: values[0],
            queue_packets_per_lp: values[1],
            channel_events_per_stream: values[2],
            service_events_per_stream: values[3],
            generator_events_per_stream: values[4],
            remote_staging_events_per_lp: values[5],
            outbox_events_total: values[6],
            tcp_receiver_ranges_per_flow: values[7],
            tcp_ledger_segments_per_flow: values[8],
            observation_events: values[9],
            worklist_entries_total: values[10],
        }
    }

    /// Renders a snapshot in the one canonical textual form.
    ///
    /// Every floor lane is written, in a fixed order, even at zero, so a snapshot's meaning does
    /// not depend on which lanes happen to be present. The three association lists arrive already
    /// sorted and deduplicated from the executor, so the same run always writes the same bytes.
    /// The trailing [`RECORD_COUNT`] line closes the file: without it truncation is undetectable.
    pub(super) fn encode(warm_start: &CapacityWarmStart) -> String {
        let mut text = String::from(HEADER);
        text.push('\n');
        for (lane, value) in FLOOR_LANES.iter().zip(floor_values(warm_start.floors)) {
            text.push_str(&format!("floor {lane} {value}\n"));
        }
        let mut records = 0_usize;
        for (key, list) in [
            ("channel", &warm_start.channel_events_by_stream),
            ("tcp-receiver", &warm_start.tcp_receiver_ranges_by_base),
            ("tcp-ledger", &warm_start.tcp_ledger_segments_by_flow),
        ] {
            for (entity, capacity) in list {
                text.push_str(&format!("{key} {entity} {capacity}\n"));
                records += 1;
            }
        }
        text.push_str(&format!("{RECORD_COUNT} {records}\n"));
        text
    }

    /// Parses the canonical form, strictly.
    ///
    /// A snapshot that cannot be read exactly is refused rather than silently downgraded to a
    /// partial hint, because a partial hint would quietly reintroduce the discarded attempts the
    /// flag exists to remove. Concretely: every floor lane must appear exactly once, the
    /// [`RECORD_COUNT`] terminator must be present and must match the association records actually
    /// read (which is what makes truncation detectable), and no capacity may exceed
    /// [`MAX_CAPACITY`] (which turns a corrupt magnitude into a typed refusal here instead of an
    /// allocator abort later, inside the planner).
    pub(super) fn decode(text: &str) -> Result<CapacityWarmStart, String> {
        let mut lines = text.lines().filter(|line| !line.trim().is_empty());
        match lines.next() {
            Some(header) if header.trim() == HEADER => {}
            other => return Err(format!("expected the header `{HEADER}`, found {other:?}")),
        }

        let mut floors = [0_usize; FLOOR_LANES.len()];
        let mut seen = [false; FLOOR_LANES.len()];
        let mut channel_events_by_stream = Vec::new();
        let mut tcp_receiver_ranges_by_base = Vec::new();
        let mut tcp_ledger_segments_by_flow = Vec::new();
        let mut declared_records = None;
        for line in lines {
            let fields = line.split_whitespace().collect::<Vec<_>>();
            if let [RECORD_COUNT, declared] = fields.as_slice() {
                let declared = declared
                    .parse::<usize>()
                    .map_err(|error| format!("`{line}` has an unreadable record count: {error}"))?;
                if declared_records.replace(declared).is_some() {
                    return Err(format!("`{line}` repeats the record count"));
                }
                continue;
            }
            let [key, entity, value] = fields.as_slice() else {
                return Err(format!(
                    "expected three whitespace-separated fields, got `{line}`"
                ));
            };
            let value = bounded_capacity(value, line)?;
            match *key {
                "floor" => {
                    let lane = FLOOR_LANES
                        .iter()
                        .position(|lane| lane == entity)
                        .ok_or_else(|| format!("`{line}` names an unknown floor lane"))?;
                    if std::mem::replace(&mut seen[lane], true) {
                        return Err(format!("`{line}` repeats a floor lane"));
                    }
                    floors[lane] = value;
                }
                "channel" | "tcp-receiver" | "tcp-ledger" => {
                    let entity = entity
                        .parse::<usize>()
                        .map_err(|error| format!("`{line}` has an unreadable entity: {error}"))?;
                    match *key {
                        "channel" => channel_events_by_stream.push((entity, value)),
                        "tcp-receiver" => tcp_receiver_ranges_by_base.push((entity, value)),
                        _ => tcp_ledger_segments_by_flow.push((entity, value)),
                    }
                }
                _ => return Err(format!("`{line}` names an unknown record kind")),
            }
        }
        if let Some(lane) = seen.iter().position(|seen| !seen) {
            return Err(format!("the floor lane `{}` is missing", FLOOR_LANES[lane]));
        }
        let records = channel_events_by_stream.len()
            + tcp_receiver_ranges_by_base.len()
            + tcp_ledger_segments_by_flow.len();
        match declared_records {
            None => {
                return Err(format!(
                    "the `{RECORD_COUNT}` terminator is missing; the snapshot is truncated or was \
                     not written by this tool"
                ));
            }
            Some(declared) if declared != records => {
                return Err(format!(
                    "the snapshot declares {declared} records but carries {records}; it is \
                     truncated or corrupt"
                ));
            }
            Some(_) => {}
        }

        Ok(CapacityWarmStart {
            floors: floors_from(floors),
            channel_events_by_stream,
            tcp_receiver_ranges_by_base,
            tcp_ledger_segments_by_flow,
        })
    }

    /// Reads one capacity and refuses it if no device in the fleet could hold it.
    fn bounded_capacity(value: &str, line: &str) -> Result<usize, String> {
        let value = value
            .parse::<u64>()
            .map_err(|error| format!("`{line}` has an unreadable capacity: {error}"))?;
        if value > MAX_CAPACITY {
            return Err(format!(
                "`{line}` names {value} records, above the {MAX_CAPACITY}-record ceiling: a single \
                 entity at that capacity claims more device memory than any machine in the fleet \
                 has, so the snapshot is corrupt"
            ));
        }
        usize::try_from(value)
            .map_err(|_| format!("`{line}` names a capacity that does not fit this target's usize"))
    }

    #[cfg(any(all(feature = "metal", target_vendor = "apple"), feature = "cuda"))]
    pub(super) fn load(path: &std::path::Path) -> Result<CapacityWarmStart, String> {
        let text = std::fs::read_to_string(path)
            .map_err(|error| format!("failed to read {}: {error}", path.display()))?;
        decode(&text).map_err(|error| format!("failed to parse {}: {error}", path.display()))
    }

    #[cfg(any(all(feature = "metal", target_vendor = "apple"), feature = "cuda"))]
    pub(super) fn dump(
        path: &std::path::Path,
        warm_start: &CapacityWarmStart,
    ) -> Result<(), String> {
        std::fs::write(path, encode(warm_start))
            .map_err(|error| format!("failed to write {}: {error}", path.display()))?;
        println!(
            "record=days_capacity_warm_start path={} channels={} \
             tcp_receiver_classes={} tcp_ledger_flows={}",
            path.display(),
            warm_start.channel_events_by_stream.len(),
            warm_start.tcp_receiver_ranges_by_base.len(),
            warm_start.tcp_ledger_segments_by_flow.len(),
        );
        Ok(())
    }
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

/// FNV-1a over the pretty `Debug` rendering of a value, streamed without materializing it.
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
        "record=days_result engine={engine} lowering_ns={lowering_ns} \
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

fn main() -> ExitCode {
    let cli = Cli::parse();
    match run(&cli) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("error: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run(cli: &Cli) -> Result<(), String> {
    cli.check_options()?;
    // Refuse an unbuilt device engine before doing any work.
    match cli.engine {
        Engine::Metal => metal_available()?,
        Engine::Cuda => cuda_available()?,
        Engine::Scalar | Engine::Cpu => {}
    }
    let lowering_started = Instant::now();
    let image = compile_config(&cli.config)
        .map_err(|error| format!("failed to lower {}: {error}", cli.config.display()))?;
    let lowering_ns = lowering_started.elapsed().as_nanos();
    match cli.engine {
        Engine::Scalar => run_scalar_engine(cli, &image, lowering_ns),
        Engine::Cpu => run_cpu_engine(cli, &image, lowering_ns),
        Engine::Metal | Engine::Cuda => {
            print_protocol(cli);
            match cli.engine {
                Engine::Metal => run_metal(cli, &image, lowering_ns),
                _ => run_cuda(cli, &image, lowering_ns),
            }
        }
    }
}

fn run_scalar_engine(cli: &Cli, image: &SimulationImage, lowering_ns: u128) -> Result<(), String> {
    let timer = Instant::now();
    let result = run_scalar(image, cli.exclusive_horizon_ns)
        .map_err(|error| format!("scalar run failed: {error:?}"))?;
    let elapsed = timer.elapsed();

    let summary = result.summary;
    println!(
        "record=days_scalar config={} stop_time_ns={} next_pending_ns={:?} pending_events={} \
         certified_lookahead_ns={:?} events={} \
         sourced_packets={} sourced_bytes={} \
         received_packets={} received_bytes={} \
         dropped_packets={} dropped_bytes={} wall_ns={}",
        cli.config.display(),
        image.stop_time_ns,
        result.pending_events.first().map(|event| event.key.time_ns),
        result.pending_events.len(),
        image
            .channels
            .iter()
            .map(|channel| channel.min_delay_ns)
            .min(),
        processed_event_count(image, &result),
        summary.sourced_packets,
        summary.sourced_bytes,
        summary.received_packets,
        summary.received_bytes,
        summary.dropped_packets,
        summary.dropped_bytes,
        elapsed.as_nanos()
    );
    print_result("scalar", &result, lowering_ns, elapsed.as_nanos());
    Ok(())
}

fn processed_event_count(image: &SimulationImage, result: &RunResult) -> u64 {
    let initial_origin_seq = image
        .host_states
        .iter()
        .map(|state| state.next_origin_seq)
        .chain(
            image
                .switch_states
                .iter()
                .map(|state| state.next_origin_seq),
        )
        .sum();
    let final_origin_seq = result
        .host_states
        .iter()
        .map(|state| state.next_origin_seq)
        .chain(
            result
                .switch_states
                .iter()
                .map(|state| state.next_origin_seq),
        )
        .sum();
    processed_event_count_from_parts(
        image.initial_events.len() as u64,
        initial_origin_seq,
        final_origin_seq,
        result.pending_events.len() as u64,
    )
}

fn processed_event_count_from_parts(
    initial_events: u64,
    initial_origin_seq: u64,
    final_origin_seq: u64,
    pending_events: u64,
) -> u64 {
    let generated_events = final_origin_seq - initial_origin_seq;
    initial_events + generated_events - pending_events
}

fn run_cpu_engine(cli: &Cli, image: &SimulationImage, lowering_ns: u128) -> Result<(), String> {
    let config = cli.cpu_config();
    let chunk = match config.granularity {
        ChunkGranularity::Static => "static".to_owned(),
        ChunkGranularity::Fixed(size) => size.to_string(),
    };
    let static_partition = match config.static_partition {
        StaticPartitionPolicy::Modulo => "modulo",
        StaticPartitionPolicy::RouteLoad => "route-load",
    };
    for repetition in 0..cli.repetitions.unwrap_or(1) {
        let timer = Instant::now();
        let run = run_cpu(image, cli.exclusive_horizon_ns, config)
            .map_err(|error| format!("CPU run failed: {error:?}"))?;
        let wall_ns = timer.elapsed().as_nanos();
        print_cpu_record(
            &cli.config,
            repetition,
            config.workers,
            &chunk,
            static_partition,
            config.straggler_threshold_events,
            config.spin_before_park,
            wall_ns,
            run.rounds.iter().map(|round| &round.semantic),
            &run.rounds,
            run.result.summary,
        );
        print_result("cpu", &run.result, lowering_ns, wall_ns);
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn print_cpu_record<'round>(
    path: &Path,
    repetition: usize,
    workers: usize,
    chunk: &str,
    static_partition: &str,
    straggler_threshold: Option<u64>,
    spin_before_park: u32,
    wall_ns: u128,
    rounds: impl Iterator<Item = &'round RoundMetrics> + Clone,
    cpu_rounds: &[CpuRoundMetrics],
    summary: days_executor::RunSummary,
) {
    let path = path.display();
    let round_count = rounds.clone().count();
    let total_events = rounds
        .clone()
        .map(|round| u128::from(round.events_processed))
        .sum::<u128>();
    let total_active_lps = rounds
        .clone()
        .map(|round| round.active_lp_count as u128)
        .sum::<u128>();
    let total_horizon_advance = rounds
        .clone()
        .map(|round| round.horizon_advance_ns)
        .sum::<u128>();
    let maximum_events = rounds
        .clone()
        .map(|round| round.events_processed)
        .max()
        .unwrap_or(0);
    let maximum_active_lps = rounds
        .clone()
        .map(|round| round.active_lp_count)
        .max()
        .unwrap_or(0);
    let maximum_lp_events = rounds
        .clone()
        .flat_map(|round| &round.lp_work)
        .map(|work| work.events_processed)
        .max()
        .unwrap_or(0);
    let mean_efficiency = mean(rounds.clone().map(|round| round.parallel_efficiency));
    let minimum_efficiency = rounds
        .clone()
        .map(|round| round.parallel_efficiency)
        .reduce(f64::min)
        .unwrap_or(1.0);
    let remote_events = rounds
        .clone()
        .map(|round| u128::from(round.messages_exchanged))
        .sum::<u128>();
    let same_time_continuations = rounds
        .clone()
        .flat_map(|round| &round.lp_work)
        .map(|work| u128::from(work.same_time_continuations))
        .sum::<u128>();
    let physical_lp_probes = rounds
        .clone()
        .map(|round| u128::from(round.physical_lp_probes))
        .sum::<u128>();
    let round_divisor = u128::try_from(round_count.max(1)).expect("round count must fit u128");
    let cpu_fields = if cpu_rounds.is_empty() {
        String::new()
    } else {
        let mean_lp_time_efficiency = mean(
            cpu_rounds
                .iter()
                .map(|round| round.lp_time_parallel_efficiency),
        );
        let mean_worker_efficiency = mean(
            cpu_rounds
                .iter()
                .map(|round| round.worker_parallel_efficiency),
        );
        let mean_worker_utilization = mean(cpu_rounds.iter().map(|round| round.worker_utilization));
        let straggler_lps = cpu_rounds
            .iter()
            .map(|round| round.partition.stragglers.len() as u128)
            .sum::<u128>();
        let bulk_chunks = cpu_rounds
            .iter()
            .map(|round| round.partition.bulk_chunks.len() as u128)
            .sum::<u128>();
        let owner_batches = cpu_rounds
            .iter()
            .map(|round| u128::from(round.owner_batch_messages))
            .sum::<u128>();
        let worker_wakes = cpu_rounds
            .iter()
            .map(|round| u128::from(round.worker_wake_messages))
            .sum::<u128>();
        let worker_completions = cpu_rounds
            .iter()
            .map(|round| u128::from(round.worker_completion_messages))
            .sum::<u128>();
        let chunk_requests = cpu_rounds
            .iter()
            .map(|round| u128::from(round.chunk_request_messages))
            .sum::<u128>();
        let classification_presence_messages = cpu_rounds
            .iter()
            .map(|round| u128::from(round.classification_presence_messages))
            .sum::<u128>();
        let classification_work_messages = cpu_rounds
            .iter()
            .map(|round| u128::from(round.classification_work_messages))
            .sum::<u128>();
        let classification_return_messages = cpu_rounds
            .iter()
            .map(|round| u128::from(round.classification_return_messages))
            .sum::<u128>();
        let owner_deliveries = cpu_rounds
            .iter()
            .map(|round| u128::from(round.owner_delivery_messages))
            .sum::<u128>();
        let owner_batches_merged = cpu_rounds
            .iter()
            .map(|round| u128::from(round.owner_batches_merged))
            .sum::<u128>();
        let early_owner_batches_merged = cpu_rounds
            .iter()
            .map(|round| u128::from(round.early_owner_batches_merged))
            .sum::<u128>();
        let owner_merge_ns = cpu_rounds
            .iter()
            .map(|round| u128::from(round.owner_merge_ns))
            .sum::<u128>();
        let early_owner_merge_ns = cpu_rounds
            .iter()
            .map(|round| u128::from(round.early_owner_merge_ns))
            .sum::<u128>();
        let pool_messages = cpu_rounds
            .iter()
            .map(|round| u128::from(round.pool_messages()))
            .sum::<u128>();
        let worker_busy_ns = cpu_rounds
            .iter()
            .flat_map(|round| &round.worker_timings)
            .map(|worker| u128::from(worker.busy_ns))
            .sum::<u128>();
        let worker_idle_ns = cpu_rounds
            .iter()
            .flat_map(|round| &round.worker_timings)
            .map(|worker| u128::from(worker.idle_ns))
            .sum::<u128>();
        let timed_rounds = cpu_rounds
            .iter()
            .filter(|round| !round.lp_timings.is_empty())
            .collect::<Vec<_>>();
        let timed_active_lps = timed_rounds
            .iter()
            .map(|round| round.lp_timings.len() as u128)
            .sum::<u128>();
        let timed_physical_lp_probes = timed_rounds
            .iter()
            .map(|round| u128::from(round.semantic.physical_lp_probes))
            .sum::<u128>();
        let timed_worker_busy_ns = timed_rounds
            .iter()
            .flat_map(|round| &round.worker_timings)
            .map(|worker| u128::from(worker.busy_ns))
            .sum::<u128>();
        let lp_busy_ns = timed_rounds
            .iter()
            .flat_map(|round| &round.lp_timings)
            .map(|lp| u128::from(lp.busy_ns))
            .sum::<u128>();
        let worker_machinery_ns = timed_worker_busy_ns.saturating_sub(lp_busy_ns);
        let coordinator_partition_ns = cpu_rounds
            .iter()
            .map(|round| u128::from(round.coordinator_partition_ns))
            .sum::<u128>();
        let worker_wait_ns = cpu_rounds
            .iter()
            .map(|round| u128::from(round.worker_wait_ns))
            .sum::<u128>();
        let coordinator_exchange_ns = cpu_rounds
            .iter()
            .map(|round| u128::from(round.coordinator_exchange_ns))
            .sum::<u128>();
        let chunks = straggler_lps.saturating_add(bulk_chunks);
        let legacy_protocol_messages_estimate = 4_u128
            .saturating_mul(workers as u128)
            .saturating_mul(round_count as u128)
            .saturating_add(3_u128.saturating_mul(chunks))
            .saturating_add(owner_batches);
        format!(
            " spin_before_park={spin_before_park} mean_lp_time_efficiency={mean_lp_time_efficiency:.6} \
             mean_worker_efficiency={mean_worker_efficiency:.6} \
             mean_worker_utilization={mean_worker_utilization:.6} \
             straggler_lps={straggler_lps} bulk_chunks={bulk_chunks} \
             owner_batches={owner_batches} worker_wakes={worker_wakes} \
             worker_completions={worker_completions} chunk_requests={chunk_requests} \
             classification_presence_messages={classification_presence_messages} \
             classification_work_messages={classification_work_messages} \
             classification_return_messages={classification_return_messages} \
             owner_deliveries={owner_deliveries} owner_batches_merged={owner_batches_merged} \
             early_owner_batches_merged={early_owner_batches_merged} \
             owner_merge_ns={owner_merge_ns} early_owner_merge_ns={early_owner_merge_ns} \
             legacy_protocol_messages_estimate={legacy_protocol_messages_estimate} \
             pool_messages_actual={pool_messages} \
             mean_pool_messages_per_round={:.3} \
             timed_rounds={} lp_busy_ns={lp_busy_ns} \
             worker_machinery_ns={worker_machinery_ns} \
             lp_busy_ns_per_timed_active_lp={:.3} \
             machinery_ns_per_timed_physical_probe={:.3} \
             wall_ns_per_physical_probe={:.3} \
             worker_busy_ns={worker_busy_ns} worker_idle_ns={worker_idle_ns} \
             coordinator_partition_ns={coordinator_partition_ns} \
             worker_wait_ns={worker_wait_ns} coordinator_exchange_ns={coordinator_exchange_ns} \
             mean_coordinator_partition_ns={:.3} mean_worker_wait_ns={:.3} \
             mean_coordinator_exchange_ns={:.3}",
            pool_messages as f64 / round_divisor as f64,
            timed_rounds.len(),
            lp_busy_ns as f64 / timed_active_lps.max(1) as f64,
            worker_machinery_ns as f64 / timed_physical_lp_probes.max(1) as f64,
            wall_ns as f64 / physical_lp_probes.max(1) as f64,
            coordinator_partition_ns as f64 / round_divisor as f64,
            worker_wait_ns as f64 / round_divisor as f64,
            coordinator_exchange_ns as f64 / round_divisor as f64,
        )
    };

    println!(
        "record=days_cpu config={path} repetition={repetition} mode=cpu workers={workers} \
         chunk={chunk} static_partition={static_partition} straggler_threshold={} \
         rounds={round_count} events={total_events} \
         mean_events_per_round={:.3} max_events_per_round={maximum_events} \
         mean_active_lps={:.3} max_active_lps={maximum_active_lps} \
         max_lp_events={maximum_lp_events} \
         mean_horizon_advance_ns={:.3} mean_parallel_efficiency={mean_efficiency:.6} \
         min_parallel_efficiency={minimum_efficiency:.6} \
         remote_events={remote_events} same_time_continuations={same_time_continuations} \
         physical_lp_probes={physical_lp_probes} \
         {cpu_fields} \
         sourced_packets={} received_packets={} dropped_packets={} wall_ns={wall_ns}",
        straggler_threshold.map_or_else(|| "none".to_owned(), |value| value.to_string()),
        total_events as f64 / round_divisor as f64,
        total_active_lps as f64 / round_divisor as f64,
        total_horizon_advance as f64 / round_divisor as f64,
        summary.sourced_packets,
        summary.received_packets,
        summary.dropped_packets,
    );
}

fn mean(values: impl Iterator<Item = f64>) -> f64 {
    let (total, count) = values.fold((0.0, 0_u64), |(total, count), value| {
        (total + value, count + 1)
    });
    if count == 0 {
        1.0
    } else {
        total / count as f64
    }
}

fn print_protocol(cli: &Cli) {
    println!(
        "record=days_protocol fixture={} engine={} \
         exclusive_horizon_ns={:?} observation_mode=Summary capacity_caps={:?} \
         max_capacity_retries={} capacity_warm_start={} dump_capacity_warm_start={} \
         channel_events_per_stream_override={:?} round_threads_per_block={}",
        cli.config.display(),
        cli.engine_name(),
        cli.exclusive_horizon_ns,
        effective_caps(cli),
        cli.max_capacity_retries(),
        cli.capacity_warm_start
            .as_ref()
            .map_or_else(|| "none".to_owned(), |path| path.display().to_string()),
        cli.dump_capacity_warm_start
            .as_ref()
            .map_or_else(|| "none".to_owned(), |path| path.display().to_string()),
        cli.channel_events_per_stream,
        effective_round_threads(cli, DEFAULT_ROUND_THREADS_PER_BLOCK),
    );
}

#[cfg(any(
    test,
    all(feature = "metal", target_vendor = "apple"),
    feature = "cuda"
))]
fn capacity_retry_counts<A>(trace: &[CapacityRetryRecord<A>]) -> (usize, usize) {
    let grown_stream_count = trace
        .iter()
        .filter_map(|record| record.stream)
        .collect::<std::collections::BTreeSet<_>>()
        .len();
    (trace.len(), grown_stream_count)
}

#[cfg(any(all(feature = "metal", target_vendor = "apple"), feature = "cuda"))]
fn channel_capacity_distribution(levels: &[days_executor::ChannelStreamCapacityLevel]) -> String {
    levels
        .iter()
        .map(|level| format!("{}:{}", level.capacity, level.stream_count))
        .collect::<Vec<_>>()
        .join(",")
}

#[cfg(all(feature = "metal", target_vendor = "apple"))]
fn metal_available() -> Result<(), String> {
    Ok(())
}

#[cfg(not(all(feature = "metal", target_vendor = "apple")))]
fn metal_available() -> Result<(), String> {
    Err(
        "--engine metal requires a build with `--features metal` on an Apple target; this binary \
         was built without it"
            .to_owned(),
    )
}

#[cfg(feature = "cuda")]
fn cuda_available() -> Result<(), String> {
    Ok(())
}

#[cfg(not(feature = "cuda"))]
fn cuda_available() -> Result<(), String> {
    Err(
        "--engine cuda requires a build with `--features cuda`; this binary was built without it"
            .to_owned(),
    )
}

#[cfg(all(feature = "metal", target_vendor = "apple"))]
fn run_metal(cli: &Cli, image: &SimulationImage, lowering_ns: u128) -> Result<(), String> {
    use days_executor::{MetalConfig, MetalExecutor, ObservationMode};

    let executor =
        MetalExecutor::new().map_err(|error| format!("Metal executor failed: {error:?}"))?;
    let warm_start = cli
        .capacity_warm_start
        .as_deref()
        .map(warm_start::load)
        .transpose()?;
    let mut config = MetalConfig {
        capacity_caps: effective_caps(cli),
        max_capacity_retries: cli.max_capacity_retries(),
        ..MetalConfig::default()
    };
    config.round_threads_per_threadgroup =
        effective_round_threads(cli, config.round_threads_per_threadgroup);
    let started = Instant::now();
    // With no `--capacity-warm-start` this is the stock entry point, unchanged.
    let run = match &warm_start {
        Some(warm_start) => executor.run_with_observations_warm_started(
            image,
            cli.exclusive_horizon_ns,
            config,
            ObservationMode::Summary,
            warm_start,
        ),
        None => executor.run_with_observations(
            image,
            cli.exclusive_horizon_ns,
            config,
            ObservationMode::Summary,
        ),
    }
    .map_err(|error| format!("Metal run failed: {error:?}"))?;
    let run_ns = started.elapsed().as_nanos();
    if let Some(path) = cli.dump_capacity_warm_start.as_deref() {
        warm_start::dump(path, &run.capacity_warm_start)?;
    }
    let (retry_count, grown_stream_count) = capacity_retry_counts(&run.capacity_retry_trace);
    let channel_capacity_distribution =
        channel_capacity_distribution(&run.channel_stream_capacity_distribution);
    println!(
        "record=days_device engine=metal wall_ns={} device_ns={} rounds={} \
         transitions={} continuation_relaunches={} retry_count={retry_count} \
         grown_stream_count={grown_stream_count} \
         channel_capacity_distribution={channel_capacity_distribution} retry_trace={:?} \
         same_time_continuations={}",
        run.wall_ns,
        run.device_ns,
        run.rounds,
        run.transitions,
        run.continuation_relaunches,
        run.capacity_retry_trace,
        run.same_time_continuations,
    );
    print_result("metal", &run.result, lowering_ns, run_ns);
    Ok(())
}

#[cfg(not(all(feature = "metal", target_vendor = "apple")))]
fn run_metal(_cli: &Cli, _image: &SimulationImage, _lowering_ns: u128) -> Result<(), String> {
    metal_available()
}

#[cfg(feature = "cuda")]
fn run_cuda(cli: &Cli, image: &SimulationImage, lowering_ns: u128) -> Result<(), String> {
    use days_executor::{CudaConfig, CudaExecutor, ObservationMode};

    let executor =
        CudaExecutor::new().map_err(|error| format!("CUDA executor failed: {error:?}"))?;
    let warm_start = cli
        .capacity_warm_start
        .as_deref()
        .map(warm_start::load)
        .transpose()?;
    let mut config = CudaConfig {
        capacity_caps: effective_caps(cli),
        max_capacity_retries: cli.max_capacity_retries(),
        ..CudaConfig::default()
    };
    config.round_threads_per_block = effective_round_threads(cli, config.round_threads_per_block);
    let started = Instant::now();
    // With no `--capacity-warm-start` this is the stock entry point, unchanged.
    let run = match &warm_start {
        Some(warm_start) => executor.run_with_observations_warm_started(
            image,
            cli.exclusive_horizon_ns,
            config,
            ObservationMode::Summary,
            warm_start,
        ),
        None => executor.run_with_observations(
            image,
            cli.exclusive_horizon_ns,
            config,
            ObservationMode::Summary,
        ),
    }
    .map_err(|error| format!("CUDA run failed: {error:?}"))?;
    let run_ns = started.elapsed().as_nanos();
    if let Some(path) = cli.dump_capacity_warm_start.as_deref() {
        warm_start::dump(path, &run.capacity_warm_start)?;
    }
    let (retry_count, grown_stream_count) = capacity_retry_counts(&run.capacity_retry_trace);
    let channel_capacity_distribution =
        channel_capacity_distribution(&run.channel_stream_capacity_distribution);
    println!(
        "record=days_device engine=cuda wall_ns={} device_ns={} rounds={} \
         transitions={} continuation_relaunches={} retry_count={retry_count} \
         grown_stream_count={grown_stream_count} \
         channel_capacity_distribution={channel_capacity_distribution} retry_trace={:?} \
         same_time_continuations={}",
        run.wall_ns,
        run.device_ns,
        run.rounds,
        run.transitions,
        run.continuation_relaunches,
        run.capacity_retry_trace,
        run.same_time_continuations,
    );
    print_result("cuda", &run.result, lowering_ns, run_ns);
    Ok(())
}

#[cfg(not(feature = "cuda"))]
fn run_cuda(_cli: &Cli, _image: &SimulationImage, _lowering_ns: u128) -> Result<(), String> {
    cuda_available()
}

#[cfg(test)]
mod tests {
    use clap::Parser;
    use days_executor::{
        CapacityRetryRecord, CapacityWarmStart, ChunkGranularity, DeviceCapacityFloors,
        StaticPartitionPolicy,
    };

    use super::{
        CAPACITY_CAPS, Cli, capacity_retry_counts, effective_caps, effective_round_threads,
        fingerprint, processed_event_count_from_parts, warm_start,
    };

    fn parse(arguments: &[&str]) -> Cli {
        let mut argv = vec!["days", "fixture.toml"];
        argv.extend_from_slice(arguments);
        Cli::try_parse_from(argv).expect("the CLI must parse")
    }

    fn retry_record(retry: usize, stream: Option<usize>) -> CapacityRetryRecord<&'static str> {
        CapacityRetryRecord {
            retry,
            arena: "test",
            node: None,
            flow: None,
            stream,
            capacity: 1,
            demand: 2,
            grown_capacity: 4,
        }
    }

    #[test]
    fn subtracts_pending_events_from_all_created_events() {
        assert_eq!(processed_event_count_from_parts(12, 12, 147, 0), 147);
    }

    #[test]
    fn counts_processed_boundary_events_while_children_remain_pending() {
        assert_eq!(processed_event_count_from_parts(8, 8, 32, 16), 16);
    }

    #[test]
    fn streaming_fingerprint_is_deterministic() {
        let value = vec![1_u64, 2, 3];
        assert_eq!(fingerprint(&value), fingerprint(&value));
        assert_ne!(fingerprint(&value), fingerprint(&vec![3_u64, 2, 1]));
    }

    #[test]
    fn measurement_overrides_are_exact() {
        for engine in ["metal", "cuda"] {
            let default = parse(&["--engine", engine]);
            assert_eq!(effective_caps(&default), CAPACITY_CAPS);
            assert_eq!(effective_round_threads(&default, 256), 256);
            assert_eq!(default.max_capacity_retries(), 16);
            assert_eq!(default.check_options(), Ok(()));

            let overridden = parse(&[
                "--engine",
                engine,
                "--channel-events-per-stream",
                "256",
                "--round-threads-per-block",
                "64",
                "--max-capacity-retries",
                "0",
            ]);
            let mut expected_caps = CAPACITY_CAPS;
            expected_caps.channel_events_per_stream = Some(256);
            assert_eq!(effective_caps(&overridden), expected_caps);
            assert_eq!(effective_round_threads(&overridden, 256), 64);
            assert_eq!(overridden.max_capacity_retries(), 0);
            assert_eq!(overridden.check_options(), Ok(()));
        }
    }

    #[test]
    fn cpu_options_map_onto_the_cpu_configuration() {
        let stock = parse(&["--engine", "cpu", "--workers", "3"]);
        assert_eq!(stock.check_options(), Ok(()));
        let config = stock.cpu_config();
        assert_eq!(config.workers, 3);
        assert_eq!(config.granularity, ChunkGranularity::Static);
        assert_eq!(config.static_partition, StaticPartitionPolicy::RouteLoad);
        assert_eq!(config.straggler_threshold_events, None);
        assert_eq!(config.dedicated_straggler_workers, 1);
        assert_eq!(config.spin_before_park, 4_096);

        let tuned = parse(&[
            "--engine",
            "cpu",
            "--workers",
            "4",
            "--chunk",
            "8",
            "--straggler-threshold",
            "100",
            "--dedicated",
            "2",
            "--spin-before-park",
            "0",
            "--static-partition",
            "modulo",
            "--repetitions",
            "3",
        ]);
        assert_eq!(tuned.check_options(), Ok(()));
        let config = tuned.cpu_config();
        assert_eq!(config.granularity, ChunkGranularity::Fixed(8));
        assert_eq!(config.straggler_threshold_events, Some(100));
        assert_eq!(config.dedicated_straggler_workers, 2);
        assert_eq!(config.spin_before_park, 0);
        assert_eq!(config.static_partition, StaticPartitionPolicy::Modulo);
        assert_eq!(tuned.repetitions, Some(3));

        let none = parse(&[
            "--engine",
            "cpu",
            "--workers",
            "1",
            "--straggler-threshold",
            "none",
        ]);
        assert_eq!(none.cpu_config().straggler_threshold_events, None);
        assert!(
            Cli::try_parse_from(["days", "f.toml", "--engine", "cpu", "--chunk", "big"]).is_err()
        );
    }

    #[test]
    fn the_cpu_engine_requires_at_least_one_worker() {
        assert!(parse(&["--engine", "cpu"]).check_options().is_err());
        assert!(
            parse(&["--engine", "cpu", "--workers", "0"])
                .check_options()
                .is_err()
        );
    }

    /// An option that does not apply to the selected engine is refused, never silently ignored.
    #[test]
    fn inapplicable_options_are_refused() {
        let error = parse(&["--engine", "scalar", "--workers", "2"])
            .check_options()
            .unwrap_err();
        assert!(error.contains("--workers"), "{error}");
        assert!(
            parse(&["--engine", "scalar", "--round-threads-per-block", "64"])
                .check_options()
                .is_err()
        );
        assert!(
            parse(&[
                "--engine",
                "cpu",
                "--workers",
                "2",
                "--max-capacity-retries",
                "0"
            ])
            .check_options()
            .is_err()
        );
        assert!(
            parse(&["--engine", "metal", "--repetitions", "2"])
                .check_options()
                .is_err()
        );
        assert!(
            parse(&["--engine", "cuda", "--workers", "2"])
                .check_options()
                .is_err()
        );
        assert_eq!(
            parse(&["--engine", "scalar", "--exclusive-horizon-ns", "5"]).check_options(),
            Ok(())
        );
        assert!(Cli::try_parse_from(["days", "f.toml", "--engine", "device"]).is_err());
        assert!(Cli::try_parse_from(["days", "f.toml"]).is_err());
    }

    /// Without the feature, the device engine is an explicit error that names the feature.
    #[test]
    fn an_unbuilt_device_engine_names_its_missing_feature() {
        #[cfg(not(all(feature = "metal", target_vendor = "apple")))]
        {
            let error = super::metal_available().unwrap_err();
            assert!(error.contains("--features metal"), "{error}");
        }
        #[cfg(not(feature = "cuda"))]
        {
            let error = super::cuda_available().unwrap_err();
            assert!(error.contains("--features cuda"), "{error}");
        }
        #[cfg(all(feature = "metal", target_vendor = "apple"))]
        assert_eq!(super::metal_available(), Ok(()));
        #[cfg(feature = "cuda")]
        assert_eq!(super::cuda_available(), Ok(()));
    }

    fn sample_warm_start() -> CapacityWarmStart {
        CapacityWarmStart {
            floors: DeviceCapacityFloors {
                queue_packets_per_lp: 2_048,
                worklist_entries_total: 90_001,
                ..DeviceCapacityFloors::default()
            },
            channel_events_by_stream: vec![(3, 512), (11, 4_096)],
            tcp_receiver_ranges_by_base: vec![(64, 129)],
            tcp_ledger_segments_by_flow: vec![(0, 24), (1, 88_472), (262_143, 4_168)],
        }
    }

    #[test]
    fn the_capacity_warm_start_snapshot_round_trips_through_its_text_form() {
        let warm_start = sample_warm_start();
        let text = warm_start::encode(&warm_start);
        assert_eq!(
            warm_start::decode(&text).expect("the canonical form must parse"),
            warm_start
        );
        // One canonical form: the same snapshot always renders the same bytes.
        assert_eq!(warm_start::encode(&warm_start), text);
        assert!(text.starts_with("days-capacity-warm-start v2\n"));
        assert_eq!(
            text.lines()
                .filter(|line| line.starts_with("floor "))
                .count(),
            warm_start::FLOOR_LANES.len(),
            "every floor lane is written even at zero, so a snapshot's meaning never depends on \
             which lanes happen to be present"
        );

        let empty = CapacityWarmStart::default();
        assert_eq!(
            warm_start::decode(&warm_start::encode(&empty)).expect("an empty snapshot must parse"),
            empty
        );
    }

    /// A snapshot that cannot be read exactly is refused, never silently downgraded: a partial
    /// hint would quietly reintroduce the discarded attempts the flag exists to remove.
    #[test]
    fn a_malformed_capacity_warm_start_is_refused_rather_than_downgraded() {
        let text = warm_start::encode(&sample_warm_start());
        assert!(warm_start::decode("").is_err());
        // A `v1` snapshot — the shape written before the terminator and the ceiling existed —
        // is refused by its header rather than read as if it had been validated.
        assert!(warm_start::decode("days-capacity-warm-start v1").is_err());
        assert!(warm_start::decode("days-capacity-warm-start v3").is_err());
        assert!(
            warm_start::decode(&text.replace("floor queue_packets_per_lp 2048\n", "")).is_err()
        );
        assert!(
            warm_start::decode(&text.replace("queue_packets_per_lp", "queue_packets")).is_err()
        );
        assert!(warm_start::decode(&format!("{text}channel 4\n")).is_err());
        assert!(warm_start::decode(&format!("{text}channel 4 nine\n")).is_err());
        assert!(warm_start::decode(&format!("{text}sockets 4 9\n")).is_err());
        assert!(
            warm_start::decode(&format!("{text}floor queue_packets_per_lp 5\n")).is_err(),
            "a repeated floor lane is ambiguous and must be refused, not last-write-wins"
        );
        // A two-field line whose keyword is not the terminator is not a record count.
        assert!(warm_start::decode(&format!("{text}sockets 9\n")).is_err());
        assert!(warm_start::decode(&format!("{text}records 3\n")).is_err());
    }

    /// Truncation, which the format could not detect until it grew a terminator.
    ///
    /// The three association lists have no declared length and the floor block is written first,
    /// so a snapshot cut anywhere after the floors used to parse **successfully** as a partial
    /// hint — exactly the silent downgrade the format claims to refuse.
    #[test]
    fn a_truncated_capacity_warm_start_is_refused_rather_than_read_as_a_partial_hint() {
        let text = warm_start::encode(&sample_warm_start());
        let lines = text.lines().collect::<Vec<_>>();
        for cut in 1..lines.len() {
            let truncated = format!("{}\n", lines[..cut].join("\n"));
            assert!(
                warm_start::decode(&truncated).is_err(),
                "a snapshot cut to {cut} of {} lines must be refused, not read as a partial hint",
                lines.len()
            );
        }
        assert!(warm_start::decode(&text).is_ok());
        // A terminator that disagrees with the body is corruption, not truncation, and is refused
        // by the same check.
        assert!(warm_start::decode(&text.replace("records 6", "records 7")).is_err());
    }

    /// A capacity no device in the fleet could hold is refused BEFORE anything is allocated.
    #[test]
    fn a_capacity_beyond_the_fleet_ceiling_is_refused_before_anything_is_allocated() {
        let text = warm_start::encode(&sample_warm_start());
        assert!(
            warm_start::decode(&text.replace("tcp-ledger 0 24", "tcp-ledger 0 9999999999999"))
                .is_err()
        );
        assert!(
            warm_start::decode(&text.replace(
                "floor worklist_entries_total 90001",
                "floor worklist_entries_total 18446744073709551615"
            ))
            .is_err()
        );
        assert!(
            warm_start::decode(&text.replace("channel 3 512", "channel 3 4294967297")).is_err(),
            "the ceiling applies to every lane, not only the per-flow one the clamp names"
        );
        assert!(
            warm_start::decode(&text.replace("tcp-receiver 64 129", "tcp-receiver 64 4294967297"))
                .is_err()
        );
        // The ceiling itself is accepted: the refusal is a bound, not a smaller cap in disguise.
        assert!(warm_start::decode(&text.replace("channel 3 512", "channel 3 4294967296")).is_ok());
    }

    #[test]
    fn retry_counts_distinguish_attempts_from_distinct_grown_streams() {
        let trace = [
            retry_record(1, Some(7)),
            retry_record(2, None),
            retry_record(3, Some(7)),
            retry_record(4, Some(9)),
        ];

        assert_eq!(capacity_retry_counts(&trace), (4, 2));
        assert_eq!(capacity_retry_counts::<&str>(&[]), (0, 0));
    }
}

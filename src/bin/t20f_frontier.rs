use std::fmt::{self, Debug, Write as _};
use std::path::PathBuf;
use std::time::Instant;

use clap::{Parser, ValueEnum};
use days::scenario::compile_config;
#[cfg(any(
    test,
    all(feature = "metal-spike", target_vendor = "apple"),
    feature = "cuda"
))]
use days_executor::CapacityRetryRecord;
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
    #[arg(long, default_value_t = 16)]
    max_capacity_retries: usize,
    /// T20l fix 3, opt-in and OFF by default: plan the first device attempt from a capacity
    /// snapshot a previous successful run of the SAME fixture emitted with
    /// `--dump-capacity-warm-start`. Omitting it reproduces stock behaviour exactly, including
    /// every discarded attempt.
    ///
    /// This is a host sizing hint, not an image or kernel cache: it changes how large the planned
    /// arenas are, and device capacity is refuse-or-run and never semantics (the T20g invariant),
    /// so the complete-state fingerprint is identical with and without it.
    #[arg(long)]
    capacity_warm_start: Option<PathBuf>,
    /// Write the capacity this run converged on, after a successful device run, so a later run of
    /// the same fixture can pass it back through `--capacity-warm-start`.
    #[arg(long)]
    dump_capacity_warm_start: Option<PathBuf>,
}

/// The `--capacity-warm-start` / `--dump-capacity-warm-start` snapshot file format (T20l fix 3).
///
/// The executor owns the semantics; this module owns exactly one canonical textual form for them,
/// so a snapshot round-trips and two runs of the same fixture write the same bytes.
#[cfg(any(
    test,
    all(feature = "metal-spike", target_vendor = "apple"),
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
    /// mutated in place: every `v1` snapshot on disk — including the one whose md5
    /// `eee955c8816e7c745570f7c6e88f204e` this round's earlier commits quote — is now refused by
    /// its header, which is the correct outcome for a file that cannot be validated.
    pub(super) const HEADER: &str = "days-capacity-warm-start v2";

    /// Terminator keyword: the number of association records the snapshot carries.
    ///
    /// The three association lists have no declared length, and the floor block is written first,
    /// so without this a snapshot truncated anywhere after the floors parses *successfully* as a
    /// partial hint — exactly the silent downgrade this format claims to refuse. Reproduced before
    /// it existed: a 1,036-line snapshot cut to 400 lines was accepted and the run then took the
    /// very retry the hint was supposed to remove.
    const RECORD_COUNT: &str = "records";

    /// The largest capacity, in records, a snapshot file may name.
    ///
    /// A capacity is a record count that the planner multiplies by a record width — the narrowest
    /// is `TCP_RANGE_WORDS = 2` u64 words — and sums into one plan. At this ceiling a **single**
    /// entity already claims `2 * 8 * 2^32 = 68.7 GB`: more than boston's entire 25.76 GB device
    /// and more than half madrid's unified pool. No value at or above it can be a capacity any
    /// device attempt converged on, so a file naming one is corrupt and is refused **here**,
    /// before anything is allocated.
    ///
    /// Without this the refusal was an allocator abort inside the planner rather than a typed
    /// error: one in-format line of a real k16 snapshot changed to `tcp-ledger 0 9999999999999`
    /// produced `memory allocation of 400000006240632 bytes failed`, and
    /// `floor worklist_entries_total 18446744073709551615` produced `capacity overflow` in
    /// `alloc::raw_vec`.
    ///
    /// This bounds the **file**, which is the untrusted surface this flag adds. A
    /// `CapacityWarmStart` constructed in process has exactly the standing `DeviceCapacityFloors`
    /// and the `max_*` overrides already have — its magnitudes are the caller's responsibility,
    /// unchanged by this round.
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
    /// [`MAX_CAPACITY`] (which is what turns a corrupt magnitude into a typed refusal here instead
    /// of an allocator abort later, inside the planner).
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

    #[cfg(any(
        all(feature = "metal-spike", target_vendor = "apple"),
        feature = "cuda"
    ))]
    pub(super) fn load(path: &std::path::Path) -> CapacityWarmStart {
        let text = std::fs::read_to_string(path)
            .unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display()));
        decode(&text).unwrap_or_else(|error| panic!("failed to parse {}: {error}", path.display()))
    }

    #[cfg(any(
        all(feature = "metal-spike", target_vendor = "apple"),
        feature = "cuda"
    ))]
    pub(super) fn dump(path: &std::path::Path, warm_start: &CapacityWarmStart) {
        std::fs::write(path, encode(warm_start))
            .unwrap_or_else(|error| panic!("failed to write {}: {error}", path.display()));
        println!(
            "record=p11_t20f_frontier_capacity_warm_start path={} channels={} \
             tcp_receiver_classes={} tcp_ledger_flows={}",
            path.display(),
            warm_start.channel_events_by_stream.len(),
            warm_start.tcp_receiver_ranges_by_base.len(),
            warm_start.tcp_ledger_segments_by_flow.len(),
        );
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

fn fingerprint(value: &impl Debug) -> Fingerprint {
    let mut writer = FingerprintWriter(Fingerprint {
        bytes: 0,
        fnv1a64: FNV1A64_OFFSET_BASIS,
    });
    write!(&mut writer, "{value:#?}").expect("debug serialization length must fit in u64");
    writer.0
}

#[cfg(any(
    test,
    all(feature = "metal-spike", target_vendor = "apple"),
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

#[cfg(any(
    all(feature = "metal-spike", target_vendor = "apple"),
    feature = "cuda"
))]
fn channel_capacity_distribution(levels: &[days_executor::ChannelStreamCapacityLevel]) -> String {
    levels
        .iter()
        .map(|level| format!("{}:{}", level.capacity, level.stream_count))
        .collect::<Vec<_>>()
        .join(",")
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
         max_capacity_retries={} capacity_warm_start={} dump_capacity_warm_start={}",
        cli.fixture.display(),
        cli.engine,
        cli.exclusive_horizon_ns,
        cli.max_capacity_retries,
        cli.capacity_warm_start
            .as_ref()
            .map_or_else(|| "none".to_owned(), |path| path.display().to_string()),
        cli.dump_capacity_warm_start
            .as_ref()
            .map_or_else(|| "none".to_owned(), |path| path.display().to_string()),
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
    let warm_start = cli.capacity_warm_start.as_deref().map(warm_start::load);
    let config = MetalConfig {
        capacity_caps: CAPACITY_CAPS,
        max_capacity_retries: cli.max_capacity_retries,
        ..MetalConfig::default()
    };
    let started = Instant::now();
    // Default OFF: with no `--capacity-warm-start` this is the stock entry point, unchanged.
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
    .expect("Metal frontier run must succeed");
    let run_ns = started.elapsed().as_nanos();
    if let Some(path) = cli.dump_capacity_warm_start.as_deref() {
        warm_start::dump(path, &run.capacity_warm_start);
    }
    let (retry_count, grown_stream_count) = capacity_retry_counts(&run.capacity_retry_trace);
    let channel_capacity_distribution =
        channel_capacity_distribution(&run.channel_stream_capacity_distribution);
    println!(
        "record=p11_t20f_frontier_device engine=metal wall_ns={} device_ns={} rounds={} \
         transitions={} retry_count={retry_count} grown_stream_count={grown_stream_count} \
         channel_capacity_distribution={channel_capacity_distribution} retry_trace={:?} \
         same_time_continuations={}",
        run.wall_ns,
        run.device_ns,
        run.rounds,
        run.transitions,
        run.capacity_retry_trace,
        run.same_time_continuations,
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
    let warm_start = cli.capacity_warm_start.as_deref().map(warm_start::load);
    let config = CudaConfig {
        capacity_caps: CAPACITY_CAPS,
        max_capacity_retries: cli.max_capacity_retries,
        ..CudaConfig::default()
    };
    let started = Instant::now();
    // Default OFF: with no `--capacity-warm-start` this is the stock entry point, unchanged.
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
    .expect("CUDA frontier run must succeed");
    let run_ns = started.elapsed().as_nanos();
    if let Some(path) = cli.dump_capacity_warm_start.as_deref() {
        warm_start::dump(path, &run.capacity_warm_start);
    }
    let (retry_count, grown_stream_count) = capacity_retry_counts(&run.capacity_retry_trace);
    let channel_capacity_distribution =
        channel_capacity_distribution(&run.channel_stream_capacity_distribution);
    println!(
        "record=p11_t20f_frontier_device engine=cuda wall_ns={} device_ns={} rounds={} \
         transitions={} retry_count={retry_count} grown_stream_count={grown_stream_count} \
         channel_capacity_distribution={channel_capacity_distribution} retry_trace={:?} \
         same_time_continuations={}",
        run.wall_ns,
        run.device_ns,
        run.rounds,
        run.transitions,
        run.capacity_retry_trace,
        run.same_time_continuations,
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
    use days_executor::{CapacityRetryRecord, CapacityWarmStart, DeviceCapacityFloors};

    use super::{capacity_retry_counts, fingerprint, warm_start};

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
    fn streaming_fingerprint_is_deterministic() {
        let value = vec![1_u64, 2, 3];
        assert_eq!(fingerprint(&value), fingerprint(&value));
        assert_ne!(fingerprint(&value), fingerprint(&vec![3_u64, 2, 1]));
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
    /// The three association lists have no declared length and the floor block is written first, so
    /// a snapshot cut anywhere after the floors used to parse **successfully** as a partial hint —
    /// exactly the silent downgrade the format claims to refuse. Reproduced on the real 1,036-line
    /// k16 snapshot: cut to 400 lines it was accepted, and the run then took the very retry the
    /// hint existed to remove.
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
    ///
    /// Both cases are the review's own reproducing snapshots, run against the real binary on the
    /// real k16 hint file. Before this bound the first produced
    /// `memory allocation of 400000006240632 bytes failed` and the second `capacity overflow` in
    /// `alloc::raw_vec` — an allocator abort inside the planner rather than a typed refusal here.
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

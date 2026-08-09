//! T21 (P12) E4 fixture gates: provenance, workload fidelity, contract, identity, anchors.
//!
//! E4 is THE FAIR ROW of the P12 roster. E1, E1-LONG and E2 put a workload *we* authored onto a
//! fabric sized like GeDES's; E4 inverts the burden and runs GeDES's OWN shipped workload — the
//! flow-size distribution, the arrival process and the pairing its published k=32 sweep row
//! produces — with Days doing the adapting. Every arm runs the same 8,192 flows, with the same
//! per-flow byte counts from the same per-flow start instants, TO COMPLETION.
//!
//! Five kinds of gate here:
//!
//! 1. **Provenance** — the committed flow table is byte-for-byte GeDES's own emitted CSV, checked
//!    by md5 against the value P09 §7.2 recorded on a different GPU architecture. If that file is
//!    ever edited, E4 stops being GeDES's workload and this test says so.
//! 2. **Workload fidelity** — every flow in the fixture carries the byte count and start instant
//!    GeDES drew for it, and the endpoint permutation is GeDES's, restated on Days' host grid.
//!    This is the machine-checked form of the report's workload-fidelity argument.
//! 3. **Contract** — the fabric parameters E4 takes from GeDES, and the ones it deliberately does
//!    not share with E1/E2, are pinned so a later edit cannot quietly change what E4 means.
//! 4. **Cross-backend identity** — scalar, CPU (two worker counts) and, on Apple hardware, Metal
//!    must return byte-identical `RunResult`s. CUDA is not reachable from this machine; that
//!    deferral is stated, not implied.
//! 5. **Frozen anchors** — the scalar `RunResult` fingerprint, at two horizons, each named on its
//!    own test per the E1-LONG precedent: a PREFIX anchor at 1.152 ms (GeDES's own F5 span, and
//!    E2's horizon, so the prefix is a meaningful span rather than an arbitrary cut) and the
//!    RUN-TO-COMPLETION anchor at the fixture's own 4 s horizon, which is the state E4's metric is
//!    actually defined on.
//!
//! The fingerprint is FNV-1a64 over the pretty `Debug` rendering of the whole `RunResult`, the same
//! function `src/bin/t20f_frontier.rs` prints, so a fingerprint here and one from that binary are
//! directly comparable.

use std::collections::BTreeMap;
use std::fmt::{self, Debug, Write as _};
use std::path::PathBuf;

use days::scenario::compile_config;
use days::topos::build::{PairingPolicy, build_graph};
use days_executor::{
    Backend, CpuConfig, ObservationMode, RunResult, SimulationImage, run_cpu_with_observations,
    run_scalar_rounds_with_observations, run_scalar_with_observations, validate,
};

const FNV1A64_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
const FNV1A64_PRIME: u64 = 0x0000_0100_0000_01b3;

const FIXTURE: &str = "e4_gedes_native_k32.toml";
const FLOW_TABLE: &str = "gedes_k32_seed41_flows.csv";

/// md5 of the GeDES-emitted flow table for `script.py`'s k=32 sweep row at `--rng_seed=41`.
///
/// Recorded on boston (RTX 4090 / sm_89 / x86_64) at GeDES tree `1376c638a0ae66116430f953da137d0c536675ad`,
/// and identical to the md5 P09 §7.2 recorded on madrid (GB10 / sm_121 / aarch64). The table is a
/// property of GeDES's seeded generator, not of either machine. §7.2 is the AS-PUBLISHED sweep row
/// (`--flow_time_range=10000000`); P09 §7.3 is the *saturated* load-matched variant and hashes to
/// `e94a96ea…`, which is NOT this table and is not E4's workload.
const FLOW_TABLE_MD5: &str = "f9eec61c11a29a998d2c849f120eb396";

// The workload, as GeDES drew it. Every one of these is arithmetic over the committed CSV.
const FLOW_COUNT: usize = 8_192;
const TOTAL_PAYLOAD_BYTES: u128 = 104_063_174_620;
const SMALLEST_FLOW_BYTES: u64 = 1_460;
const LARGEST_FLOW_BYTES: u64 = 29_200_000;
const LATEST_ARRIVAL_NS: u64 = 95_787_147;
const EARLIEST_ARRIVAL_NS: u64 = 214;
const SEGMENT_BYTES: u64 = 1_460;

// The fabric, as GeDES fixes it.
const FT_K: i64 = 32;
const HOSTS_PER_EDGE: i64 = 16;
const EDGE_SWITCHES: usize = 512;
const PORT_RATE_BPS: i64 = 100_000_000_000;
const PROPAGATION_NS: i64 = 1_000;
/// E1/E2's depth, NOT GeDES's `Switch_DEFAULT_EGRESS_QUEUE_SIZE = 200` — see
/// `e4_keeps_the_family_queue_depth_and_says_why`.
const SWITCH_CAPACITY_PACKETS: i64 = 1_024;

/// E4's `duration`, in nanoseconds. A NON-BINDING upper bound, not a measurement window.
const HORIZON_NS: u64 = 4_000_000_000;
/// Days' RTO floor (`executor/src/tcp.rs` `MIN_RTO_NS`) — RFC 6298's one second.
const RTO_FLOOR_NS: u64 = 1_000_000_000;
/// The latest armed retransmission deadline MEASURED on the REJECTED 200-packet-queue variant of
/// this workload, where 7 of 8,192 flows were parked on timers. The committed fixture drops
/// nothing, so this is the cost-of-one-loss datum the horizon is sized against, not an observation
/// of the committed fixture.
const MEASURED_LATEST_RTO_DEADLINE_NS: u64 = 1_013_783_350;
/// The MEASURED drain instant of the committed fixture on scalar: 8,192/8,192 complete, zero drops,
/// zero retransmitted bytes, nothing resident, nothing pending, over 78,999 rounds.
///
/// THIS IS E4'S OWN METRIC and it is ASSERTED against a run by `e4_runs_to_completion_and_drains`,
/// not merely quoted: neither `RunResult` nor `RunSummary` carries a time field, so the two frozen
/// anchors cannot cover it, and without a gate the executor's TCP timing could move while all the
/// other gates stayed green and five committed artifacts quietly went false.
const MEASURED_DRAIN_NS: u64 = 96_054_393;
/// The MEASURED executor round count of that same run. Gated with `MEASURED_DRAIN_NS`, and for the
/// same reason: `RoundMetrics` is instrumentation, not fingerprinted state.
const MEASURED_ROUNDS: usize = 78_999;

/// The documented prefix the scalar anchor is frozen at: GeDES's own F5 span, and E2's horizon.
const PREFIX_ANCHOR_NS: u64 = 1_152_000;

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

fn fixture_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("configs/benchmarks/p12")
        .join(name)
}

fn fixture_table(name: &str) -> toml::Table {
    std::fs::read_to_string(fixture_path(name))
        .unwrap_or_else(|error| panic!("{name} must be readable: {error}"))
        .parse::<toml::Table>()
        .unwrap_or_else(|error| panic!("{name} must parse: {error}"))
}

fn lower(name: &str) -> SimulationImage {
    let path = fixture_path(name);
    compile_config(&path).unwrap_or_else(|error| panic!("{} must lower: {error}", path.display()))
}

fn scalar_run(image: &SimulationImage, horizon_ns: Option<u64>) -> RunResult {
    run_scalar_with_observations(image, horizon_ns, ObservationMode::Summary)
        .expect("scalar run must succeed")
}

/// One E4 flow as the committed fixture states it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct FixtureFlow {
    source: u64,
    target: u64,
    size_bytes: u64,
    initial_delay_ns: u64,
}

/// Parses `initial_delay` back to exact nanoseconds from the literal text.
///
/// The fixture writes nine fraction digits precisely so the value round-trips through decimal
/// without touching a float, and `toml`'s own f64 parse would defeat that. Reading the literal is
/// what Days' lowering does too (`ExactDecimal` is a source span), so this is the same arithmetic.
fn exact_nanoseconds(literal: &str) -> u64 {
    let (whole, fraction) = literal
        .split_once('.')
        .unwrap_or_else(|| panic!("initial_delay `{literal}` must be a decimal-seconds literal"));
    assert_eq!(
        fraction.len(),
        9,
        "initial_delay `{literal}` must carry exactly nine fraction digits"
    );
    whole.parse::<u64>().expect("whole seconds") * 1_000_000_000
        + fraction.parse::<u64>().expect("fraction nanoseconds")
}

/// The 8,192 explicit flows, read out of the committed fixture text.
fn fixture_flows() -> Vec<FixtureFlow> {
    let text = std::fs::read_to_string(fixture_path(FIXTURE)).expect("E4 fixture must be readable");
    let table = text.parse::<toml::Table>().expect("E4 fixture must parse");
    let flows = table["flow"].as_array().expect("E4 must use `[[flow]]`");

    // `initial_delay` has to come from the raw text, not from `toml`'s f64.
    let mut delays = Vec::with_capacity(flows.len());
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("traffic = {initial_delay = ") {
            let literal = rest.split(',').next().expect("initial_delay literal");
            delays.push(exact_nanoseconds(literal));
        }
    }
    assert_eq!(
        delays.len(),
        flows.len(),
        "every `[[flow]]` must carry one `initial_delay` literal on its own traffic line"
    );

    flows
        .iter()
        .zip(delays)
        .map(|(flow, initial_delay_ns)| {
            assert_eq!(
                flow["flow_type"].as_str(),
                Some("TCP"),
                "every E4 flow is closed-loop TCP"
            );
            let graph = flow["graph"].as_array().expect("graph");
            assert_eq!(graph.len(), 1, "one endpoint pair per explicit flow");
            let pair = graph[0].as_array().expect("endpoint pair");
            let traffic = flow["traffic"].as_table().expect("traffic");
            FixtureFlow {
                source: pair[0].as_integer().expect("source") as u64,
                target: pair[1].as_integer().expect("target") as u64,
                size_bytes: traffic["size"].as_integer().expect("size") as u64,
                initial_delay_ns,
            }
        })
        .collect()
}

/// GeDES's emitted flow table, keyed by GeDES host index: `(target index, bytes, start ns)`.
fn gedes_flow_table() -> BTreeMap<u64, (u64, u64, u64)> {
    const IP_BASE: u64 = 0xC0A8_0000;
    const IP_GROUP_SIZE: u64 = 48;
    let text =
        std::fs::read_to_string(fixture_path(FLOW_TABLE)).expect("flow table must be readable");
    let host_index = |ip: u64| {
        let offset = ip - IP_BASE;
        let (group, ordinal) = (offset / IP_GROUP_SIZE, offset % IP_GROUP_SIZE);
        assert!(ordinal < HOSTS_PER_EDGE as u64, "rack ordinal out of range");
        group * HOSTS_PER_EDGE as u64 + ordinal
    };
    let mut rows = BTreeMap::new();
    for line in text.lines().skip(1) {
        let fields = line.split(',').collect::<Vec<_>>();
        let parse = |index: usize| fields[index].parse::<u64>().expect("numeric CSV field");
        rows.insert(
            host_index(parse(0)),
            (host_index(parse(1)), parse(2), parse(3)),
        );
    }
    rows
}

/// GeDES's switch-major host index -> Days' ordinal-major host id.
fn days_host_id(gedes_index: u64) -> u64 {
    let (switch, ordinal) = (
        gedes_index / HOSTS_PER_EDGE as u64,
        gedes_index % HOSTS_PER_EDGE as u64,
    );
    ordinal * EDGE_SWITCHES as u64 + switch
}

/// Runs the fixture on every backend this machine can reach and returns the shared result.
///
/// Scalar is the reference. CPU runs at two worker counts because the worker count is a host
/// scheduling parameter and must be invisible in complete state. Metal joins on Apple hardware.
/// CUDA is NOT reachable here; that deferral is stated, not implied.
fn identical_across_local_backends(
    name: &str,
    image: &SimulationImage,
    horizon_ns: Option<u64>,
) -> Fingerprint {
    validate(image, Backend::Scalar).unwrap_or_else(|error| panic!("{name} scalar: {error}"));
    let scalar = scalar_run(image, horizon_ns);
    let reference = fingerprint(&scalar);

    for workers in [2_usize, 4] {
        validate(image, Backend::Cpu { workers })
            .unwrap_or_else(|error| panic!("{name} cpu {workers}: {error}"));
        let cpu = run_cpu_with_observations(
            image,
            horizon_ns,
            CpuConfig {
                workers,
                ..CpuConfig::default()
            },
            ObservationMode::Summary,
        )
        .unwrap_or_else(|error| panic!("{name} cpu {workers} run: {error}"));
        assert_eq!(
            cpu.result, scalar,
            "{name}: CPU with {workers} workers diverged from scalar"
        );
        assert_eq!(fingerprint(&cpu.result), reference);
    }

    #[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
    {
        use days_executor::{MetalConfig, MetalExecutor};
        let executor = MetalExecutor::new().expect("Metal executor must initialize");
        let run = executor
            .run_with_observations(
                image,
                horizon_ns,
                MetalConfig::default(),
                ObservationMode::Summary,
            )
            .unwrap_or_else(|error| panic!("{name} metal run: {error}"));
        assert_eq!(run.result, scalar, "{name}: Metal diverged from scalar");
        assert_eq!(fingerprint(&run.result), reference);
    }

    reference
}

/// Scalar + CPU (two worker counts) identity, for the horizon Metal cannot reach.
///
/// Used ONLY by the run-to-completion anchor, and only because Metal refuses that run — see
/// `e4_completion_on_metal_exhausts_the_tcp_segment_ledger`, which pins the refusal so this
/// exclusion can never become a silent one. Every other E4 gate, including the prefix anchor,
/// uses `identical_across_local_backends` and Metal is in it.
fn identical_across_cpu_backends(
    name: &str,
    image: &SimulationImage,
    horizon_ns: Option<u64>,
) -> Fingerprint {
    validate(image, Backend::Scalar).unwrap_or_else(|error| panic!("{name} scalar: {error}"));
    let scalar = scalar_run(image, horizon_ns);
    let reference = fingerprint(&scalar);
    for workers in [2_usize, 4] {
        validate(image, Backend::Cpu { workers })
            .unwrap_or_else(|error| panic!("{name} cpu {workers}: {error}"));
        let cpu = run_cpu_with_observations(
            image,
            horizon_ns,
            CpuConfig {
                workers,
                ..CpuConfig::default()
            },
            ObservationMode::Summary,
        )
        .unwrap_or_else(|error| panic!("{name} cpu {workers} run: {error}"));
        assert_eq!(
            cpu.result, scalar,
            "{name}: CPU with {workers} workers diverged from scalar"
        );
        assert_eq!(fingerprint(&cpu.result), reference);
    }
    reference
}

// -------------------------------------------------------------------------------------------
// 1. Provenance.
// -------------------------------------------------------------------------------------------

/// The committed flow table is GeDES's own output, unedited.
///
/// This is the root of E4's whole claim. If the CSV is regenerated, hand-trimmed, re-sorted or
/// "cleaned up", E4 is no longer running GeDES's workload and every fidelity statement downstream
/// of it becomes false without any other test noticing.
#[test]
fn the_flow_table_is_gedes_own_emitted_output() {
    let bytes = std::fs::read(fixture_path(FLOW_TABLE)).expect("flow table must be readable");
    assert_eq!(
        md5_hex(&bytes),
        FLOW_TABLE_MD5,
        "{FLOW_TABLE} is not the CSV GeDES emitted for `script.py`'s k=32 row at --rng_seed=41"
    );
    let text = String::from_utf8(bytes).expect("flow table is ASCII CSV");
    let mut lines = text.lines();
    assert_eq!(
        lines.next(),
        Some("source_ip,dst_ip,flow_size,start_timestamp,completed_timestamp,flow_completion_time"),
        "GeDES's own header must be present verbatim"
    );
    assert_eq!(lines.count(), FLOW_COUNT, "one row per host, k^3/4 = 8192");
}

/// The generator reproduces the committed fixture byte for byte.
#[test]
fn the_generator_reproduces_the_committed_fixture() {
    let directory = std::env::temp_dir().join(format!("days-t21-e4-{}", std::process::id()));
    std::fs::create_dir_all(&directory).expect("temporary output directory");
    let generator = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("configs/benchmarks/p12/gen_e4_gedes_native.py");
    let status = std::process::Command::new("python3")
        .arg(&generator)
        .arg("--out-dir")
        .arg(&directory)
        .status()
        .expect("python3 must be available to regenerate E4");
    assert!(status.success(), "E4 generator failed");
    let regenerated = std::fs::read(directory.join(FIXTURE)).expect("regenerated fixture");
    let committed = std::fs::read(fixture_path(FIXTURE)).expect("committed fixture");
    assert!(
        regenerated == committed,
        "{FIXTURE}: the committed fixture is not what the generator produces"
    );
    let _ = std::fs::remove_dir_all(&directory);
}

// -------------------------------------------------------------------------------------------
// 2. Workload fidelity.
// -------------------------------------------------------------------------------------------

/// EVERY flow carries the byte count and start instant GeDES drew for it — no aggregate stands in.
///
/// The byte and completion gates E4 publishes are sums, and a sum can be right while the
/// distribution is wrong. This walks all 8,192 flows and pins the per-flow correspondence, so the
/// claim "Days ran GeDES's workload" is checked at the resolution it is made at.
#[test]
fn every_e4_flow_is_gedes_own_flow() {
    let gedes = gedes_flow_table();
    assert_eq!(gedes.len(), FLOW_COUNT);
    let fixture = fixture_flows();
    assert_eq!(fixture.len(), FLOW_COUNT);

    let by_source = fixture
        .iter()
        .map(|flow| (flow.source, *flow))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        by_source.len(),
        FLOW_COUNT,
        "one flow per source host, as GeDES generates one connection per node"
    );

    for (gedes_source, (gedes_target, size_bytes, start_ns)) in gedes {
        let source = days_host_id(gedes_source);
        let flow = by_source
            .get(&source)
            .unwrap_or_else(|| panic!("GeDES host {gedes_source} has no E4 flow"));
        assert_eq!(
            flow.target,
            days_host_id(gedes_target),
            "GeDES host {gedes_source}: endpoint does not map to GeDES's peer"
        );
        assert_eq!(
            flow.size_bytes, size_bytes,
            "GeDES host {gedes_source}: byte count is not GeDES's flow_size"
        );
        assert_eq!(
            flow.initial_delay_ns, start_ns,
            "GeDES host {gedes_source}: start instant is not GeDES's start_timestamp"
        );
    }
}

/// The workload totals E4's count gates are stated in terms of.
///
/// These are the numbers every arm's report must reproduce: if an arm delivers a different total,
/// it did not run E4.
#[test]
fn e4_workload_totals_are_gedes_totals() {
    let flows = fixture_flows();
    let total = flows
        .iter()
        .map(|flow| u128::from(flow.size_bytes))
        .sum::<u128>();
    assert_eq!(
        total, TOTAL_PAYLOAD_BYTES,
        "E4's byte gate is the exact sum of GeDES's own flow_size column"
    );
    assert_eq!(
        total % u128::from(SEGMENT_BYTES),
        0,
        "GeDES draws flow sizes in 1,460 B units, so the total is a whole number of segments"
    );
    assert_eq!(
        flows.iter().map(|flow| flow.size_bytes).min(),
        Some(SMALLEST_FLOW_BYTES)
    );
    assert_eq!(
        flows.iter().map(|flow| flow.size_bytes).max(),
        Some(LARGEST_FLOW_BYTES),
        "the largest flow is GeDES's 2 x average_flow_size truncation, 20,000 segments"
    );
    assert_eq!(
        flows.iter().map(|flow| flow.initial_delay_ns).max(),
        Some(LATEST_ARRIVAL_NS)
    );
    assert_eq!(
        flows.iter().map(|flow| flow.initial_delay_ns).min(),
        Some(EARLIEST_ARRIVAL_NS),
        "the earliest of GeDES's 8,192 exponential arrivals; NOT zero, so E4 has no flow that \
         starts at the origin and its very first event is already GeDES's draw"
    );
}

/// E4's explicit endpoint list IS Days' `SwitchOffsetHalf`, computed independently.
///
/// E4 has to state its matrix flow by flow because each flow needs its own size and start, so the
/// permutation is written out rather than named. That is exactly the situation in which an index
/// error survives every other check — the file would still be a valid 8,192-flow permutation, just
/// the wrong one, and T21 §2.3 records that copying GeDES's switch-major arithmetic onto Days'
/// ordinal-major hosts produces a RACK-LOCAL matrix that looks entirely plausible. So the pairs are
/// compared against the policy Days computes from the attachment grid itself.
#[test]
fn e4_endpoints_are_the_switch_offset_half_permutation() {
    let (_, attachments) = build_graph(
        fixture_path(FIXTURE)
            .to_str()
            .expect("fixture path is UTF-8"),
    )
    .expect("E4 topology must build");
    assert_eq!(attachments.len(), FLOW_COUNT);
    let structural = attachments
        .structural_flow_pairs(PairingPolicy::SwitchOffsetHalf, FLOW_COUNT)
        .expect("SwitchOffsetHalf must be computable on E4's grid");

    let fixture = fixture_flows();
    let stated = fixture
        .iter()
        .map(|flow| (flow.source as usize, flow.target as usize))
        .collect::<Vec<_>>();
    assert_eq!(
        stated, structural,
        "E4's explicit endpoints are not the SwitchOffsetHalf permutation GeDES's i <-> i + N/2 \
         pairing becomes on Days' host grid"
    );
    assert_eq!(
        stated[0].0, 0,
        "flows are emitted in ascending Days source host id"
    );
}

// -------------------------------------------------------------------------------------------
// 3. Contract.
// -------------------------------------------------------------------------------------------

/// The fabric parameters E4 takes from GeDES.
#[test]
fn e4_fabric_is_gedes_fabric_where_days_can_express_it() {
    let config = fixture_table(FIXTURE);
    assert_eq!(config["topology"]["category"].as_str(), Some("FatTree"));
    assert_eq!(config["topology"]["fat_tree"]["k"].as_integer(), Some(FT_K));
    assert_eq!(
        config["topology"]["fat_tree"]["hosts_per_edge"].as_integer(),
        Some(HOSTS_PER_EDGE),
        "8,192 hosts = k^3/4, GeDES's own host count"
    );
    assert_eq!(
        config["switch"]["port_rate"].as_integer(),
        Some(PORT_RATE_BPS),
        "GeDES's tx_rate = 100 bits/ns"
    );
    assert_eq!(
        config["link"]["propagation_ns"].as_integer(),
        Some(PROPAGATION_NS),
        "GeDES's popogation_delay = 1000 ns per link"
    );
    assert_eq!(config["switch"]["discipline"].as_str(), Some("FIFO"));
    assert_eq!(
        config["switch"]["drop"].as_str(),
        Some("TailDrop"),
        "GeDES's switch queues are tail-drop; it has no AQM on this path"
    );
    assert_eq!(
        config["routing"]["policy"].as_str(),
        Some("FatTreeEcmp"),
        "equal-cost multipath, stated. GeDES sprays per packet by round robin and Days selects one \
         path per flow by hash; both are ECMP over the same fat tree and neither is the other, \
         which is a disclosure, not a defect. Days' single-path table is not an option under an \
         offset permutation."
    );

    // Every flow is closed-loop Reno on a 1,460 B segment — GeDES's MSS.
    let text = std::fs::read_to_string(fixture_path(FIXTURE)).expect("fixture readable");
    assert_eq!(
        text.matches(r#"pkt_size_dist = {type = "DiscreteUniform", low = 1460, high = 1460}"#)
            .count(),
        FLOW_COUNT
    );
    assert_eq!(
        text.matches(r#"tcp = {cc_algorithm = "TCPReno"}"#).count(),
        FLOW_COUNT
    );
}

/// E4 keeps E1/E2's queue depth, and the one place it does NOT follow GeDES is stated here.
///
/// E4's subject is GeDES's WORKLOAD — flow sizes, pairings, arrivals, on a k = 32 fat tree. The
/// queue depth is a fabric parameter E1 and E2 already fix at 1,024 and disclose, so E4 keeps it
/// and stays differenceable against its own family.
///
/// THE 200-PACKET VARIANT WAS AUTHORED, RUN AND REJECTED, AND THE REJECTION IS A MEASURED RESULT.
/// At GeDES's own `Switch_DEFAULT_EGRESS_QUEUE_SIZE = 200` this workload does not complete on Days:
/// 776 tail drops out of 142,738,118 sourced packets (0.00054 %) concentrated in seven flows, which
/// then advance ~39 kB per retransmission timeout — 8,185/8,192 complete by 150 ms, 8,189/8,192 by
/// 4 s, ~400 s of tail still owed. Two mechanisms compose: the 1 s RTO floor against a measured
/// 25–36 µs RTT, and `scalar.rs`'s `allowance = cwnd − bytes_in_flight` after a timeout collapses
/// `cwnd` to one MSS without releasing the in-flight bytes. `evidence/P12/e4-authoring.md` carries
/// the full record; the variant is owed as its own capability row and must not be folded into the
/// completion-time row.
#[test]
fn e4_keeps_the_family_queue_depth_and_says_why() {
    let e4 = fixture_table(FIXTURE);
    let e2 = fixture_table("e2_closed_k32_tcp_reno.toml");
    assert_eq!(
        e4["switch"]["capacity"].as_integer(),
        Some(SWITCH_CAPACITY_PACKETS)
    );
    assert_eq!(
        e4["switch"]["capacity"], e2["switch"]["capacity"],
        "E4 and E2 must share the family queue depth; the GeDES-depth variant is a separate row"
    );
    assert_ne!(
        SWITCH_CAPACITY_PACKETS, 200,
        "200 is GeDES's egress depth and is the value this workload provably does not complete at"
    );
    // The rest of the fabric E4 and E2 share, so a reader can name the differences exactly: the
    // traffic, and nothing else.
    for key in ["port_rate", "weights", "discipline", "drop"] {
        assert_eq!(
            e4["switch"].get(key),
            e2["switch"].get(key),
            "E4 and E2 must agree on switch {key}"
        );
    }
    assert_eq!(e4["topology"], e2["topology"]);
    assert_eq!(e4["link"], e2["link"]);
    assert_eq!(e4["routing"], e2["routing"]);
}

/// The horizon is a NON-BINDING upper bound, and the arithmetic that makes it non-binding is here.
///
/// E4's semantics are run-to-completion: the run ends when the fabric drains, and Days has no
/// stop-when-idle control, so `duration` has to be past the drain instant. A horizon that turned
/// out to be binding would silently convert E4 from a completion-time fixture into a fixed-window
/// one, and `e4_runs_to_completion_and_drains` is what detects that at run time.
///
/// THE SIZING IS SET BY THE RTO FLOOR, NOT BY THE WORKLOAD, and that is the whole content of this
/// test. The workload's own arithmetic is small — the last flow ARRIVES at 95.787147 ms and the
/// largest flow needs 2.336 ms of serialisation at 100 Gbit/s, so 98.123147 ms bounds an
/// uncongested drain, and the committed fixture MEASURES a drain at 96,054,393 ns, drop-free.
///
/// The horizon is nonetheless forty times that, because the cost of ONE lost segment is a full
/// second: Days' RTO floor is one second (RFC 6298) against a measured 25–36 µs RTT, and after a
/// timeout `cwnd` collapses to one MSS without releasing the in-flight bytes, so a flow that loses
/// a burst advances only as fast as its own cumulative ACK recovers the window — MEASURED at
/// ~39 kB (about 27 segments) per one-second timeout, not one segment per second. That is not
/// hypothetical — it was measured on
/// the rejected 200-packet-queue variant, where seven flows parked on timers out to
/// 1,013,783,350 ns. A horizon sized to the drain would turn a single drop into a SILENT
/// truncation instead of a loud one, and the slack is free: 78,999 rounds for a 4 s horizon whose
/// fabric is empty after 96 ms.
#[test]
fn e4_horizon_is_a_non_binding_upper_bound() {
    let config = fixture_table(FIXTURE);
    let duration = config["duration"]
        .as_float()
        .expect("duration is a decimal-seconds literal");
    assert_eq!(duration, HORIZON_NS as f64 / 1e9);

    // (1) The workload's own bound — necessary, and nowhere near sufficient.
    let serialisation_ns = LARGEST_FLOW_BYTES * 8 * 1_000_000_000 / PORT_RATE_BPS as u64;
    assert_eq!(serialisation_ns, 2_336_000, "29.2 MB at 100 Gbit/s");
    let uncongested_drain_ns = LATEST_ARRIVAL_NS + serialisation_ns;
    assert_eq!(uncongested_drain_ns, 98_123_147);
    assert!(
        uncongested_drain_ns < HORIZON_NS,
        "the horizon must exceed the uncongested drain bound"
    );
    assert!(
        MEASURED_DRAIN_NS < uncongested_drain_ns,
        "the measured drain must sit under its own uncongested bound, or the bound is wrong"
    );

    // (2) The bound that actually binds: a lost segment costs at least one RTO floor, and the
    // measured worst deadline on this workload is already past it.
    const {
        assert!(
            MEASURED_LATEST_RTO_DEADLINE_NS > RTO_FLOOR_NS,
            "the measured deadline must be at or beyond the floor, or the constant is stale"
        );
    }
    assert!(
        MEASURED_LATEST_RTO_DEADLINE_NS > 10 * uncongested_drain_ns,
        "the RTO floor, not the workload, is what sizes this fixture — if that ever stops being \
         true the header's explanation has to be rewritten"
    );

    // (3) One full x2 backoff past the measured deadline must still fit, so a single further
    // timeout is recoverable inside the horizon. A SECOND backoff deliberately does not fit: a
    // flow needing three consecutive timeouts is a finding and must fail the gate, not be absorbed.
    let one_backoff_ns = MEASURED_LATEST_RTO_DEADLINE_NS + 2 * RTO_FLOOR_NS;
    let two_backoffs_ns = one_backoff_ns + 4 * RTO_FLOOR_NS;
    assert!(one_backoff_ns < HORIZON_NS, "one backoff must fit");
    assert!(
        two_backoffs_ns > HORIZON_NS,
        "two backoffs must NOT fit; an unbounded horizon would absorb a pathology instead of \
         reporting it"
    );
}

/// E4 lowers, and lowers to the workload it states.
#[test]
fn e4_lowers_to_gedes_byte_demand() {
    let image = lower(FIXTURE);
    let mut flows = 0_usize;
    let mut demanded = 0_u128;
    for host in &image.host_states {
        for generator in &host.generators {
            if let days_executor::FlowGeneratorKind::Tcp(tcp) = generator.kind {
                flows += 1;
                demanded += u128::from(tcp.total_bytes);
                assert_eq!(tcp.mss_bytes, SEGMENT_BYTES, "GeDES's MSS");
            }
        }
    }
    assert_eq!(
        flows, FLOW_COUNT,
        "8,192 closed-loop generators, one per host"
    );
    assert_eq!(
        demanded, TOTAL_PAYLOAD_BYTES,
        "the lowered image must demand exactly GeDES's byte total"
    );
}

/// `arr_dist` has NO effect on E4's lowered image — which is exactly why legacy cannot run E4.
///
/// Days AGO states this in `src/scenario/compile.rs`: "Closed-loop TCP owns all post-start send
/// timing. The legacy `arr_dist` field is accepted for source-file compatibility but has no
/// executor semantic effect." Frozen legacy does NOT share that semantic: its TCP source draws its
/// application data from the same distribution-driven generator the open-loop source uses, so
/// `arr_dist` is a hard send-rate cap there. Measured on the E4 file itself
/// (`evidence/P12/e4-authoring.md` §8.2): at E4's OWN 4 s horizon legacy's `Total packets
/// processed` scales inversely with the interval over four decades — 98,280 at 1 s, 196,536 at
/// 500 ms, 981,330 at 100 ms, 9,670,380 at 10 ms — and then, at 1 ms, where the extrapolation
/// would first approach the workload's own 71,276,147 segments, legacy PANICS in its port
/// scheduler (`legacy/src/schedulers/port.rs:425`, `InvalidScheduledTime`) and reports 0 packets
/// while still exiting 0.
///
/// This test is the Days-AGO half of that finding, and it is what makes it a SEMANTIC divergence
/// rather than a fixture typo: the same edit that moves legacy's packet count by two orders of
/// magnitude, or crashes it outright, moves nothing at all in the image E4's own arms execute.
#[test]
fn arr_dist_does_not_reach_e4s_lowered_image() {
    let committed = std::fs::read_to_string(fixture_path(FIXTURE)).expect("fixture readable");
    let respelled = committed.replace(
        r#"arr_dist = {type = "Uniform", low = 1.0, high = 1.0}"#,
        r#"arr_dist = {type = "Uniform", low = 0.0000001168, high = 0.0000001168}"#,
    );
    assert_eq!(
        respelled.matches("0.0000001168").count(),
        FLOW_COUNT * 2,
        "the respelling must touch every flow, or the comparison proves nothing"
    );

    let directory = std::env::temp_dir().join(format!("days-t21-e4-arr-{}", std::process::id()));
    std::fs::create_dir_all(&directory).expect("temporary directory");
    let path = directory.join(FIXTURE);
    std::fs::write(&path, respelled).expect("respelled fixture");
    let respelled_image = compile_config(&path).expect("respelled fixture must lower");
    let _ = std::fs::remove_dir_all(&directory);

    assert_eq!(
        lower(FIXTURE),
        respelled_image,
        "arr_dist reached E4's lowered image; the legacy-vs-AGO divergence recorded in \
         evidence/P12/e4-authoring.md would have to be restated"
    );
}

// -------------------------------------------------------------------------------------------
// 4/5. Identity and anchors.
// -------------------------------------------------------------------------------------------

/// PREFIX anchor: scalar/CPU/Metal identity over E4's first 1.152 ms.
///
/// 1.152 ms is not an arbitrary cut. It is GeDES's own F5 span and E2's horizon, so the prefix is
/// the window in which GeDES's published k=32 row was last characterised — the campaign reports
/// 284 of 8,192 flows completed there, i.e. the prefix is the arrival transient, not the drained
/// fabric. E4's run-to-completion state is anchored separately and is NOT this fingerprint.
#[test]
#[ignore = "explicit P12 E4 PREFIX anchor: 1.152 ms of the 4 s horizon, 8,192 TCP flows on k=32"]
fn e4_prefix_anchor_is_identical_across_local_backends() {
    let image = lower(FIXTURE);
    let observed = identical_across_local_backends(FIXTURE, &image, Some(PREFIX_ANCHOR_NS));
    println!(
        "E4 PREFIX anchor @ {PREFIX_ANCHOR_NS} ns: bytes={} fnv1a64={:016x}",
        observed.bytes, observed.fnv1a64
    );
    assert_anchor(
        FIXTURE,
        observed,
        E4_PREFIX_ANCHOR_BYTES,
        E4_PREFIX_ANCHOR_FNV,
    );
}

/// RUN-TO-COMPLETION anchor: the state E4's metric is actually defined on.
///
/// Separate from the prefix anchor because they are different states, not because the full run is
/// out of reach: it is not. The measurement round owns E4's timing; this gate owns its state.
///
/// The fingerprint is SMALLER than the prefix anchor's because this is a DRAINED image — no
/// resident packets, no pending events, no armed timers. That is the fixture working as specified,
/// not a truncated run.
///
/// SCALAR + CPU ONLY, because Metal cannot reach this state: it exhausts its per-flow TCP
/// segment-ledger capacity. Metal IS in the prefix anchor, so the image is expressible on it;
/// what it will not do is finish. `e4_completion_on_metal_exhausts_the_tcp_segment_ledger` pins
/// the refusal verbatim, and `evidence/P12/e4-authoring.md` §6.4 hands it off as a Metal-lane
/// blocker. It is neither diagnosed nor tuned around here — `MetalConfig::default()` throughout.
#[test]
#[ignore = "explicit P12 E4 RUN-TO-COMPLETION anchor: full 4 s horizon, 104 GB of payload"]
fn e4_completion_anchor_is_identical_across_local_backends() {
    let image = lower(FIXTURE);
    let observed = identical_across_cpu_backends(FIXTURE, &image, None);
    println!(
        "E4 COMPLETION anchor (scalar + CPU x2/x4; Metal exhausts its ledger, see the doc \
         comment): bytes={} fnv1a64={:016x}",
        observed.bytes, observed.fnv1a64
    );
    assert_anchor(
        FIXTURE,
        observed,
        E4_COMPLETION_ANCHOR_BYTES,
        E4_COMPLETION_ANCHOR_FNV,
    );
}

/// The run-to-completion COUNT GATES, checked on scalar state.
///
/// These are E4's definition of a valid run and every arm reproduces them in its own vocabulary:
/// 8,192 of 8,192 flows byte-complete, the exact GeDES byte total delivered, nothing dropped
/// unacknowledged, nothing left resident, and the drain strictly inside the horizon.
///
/// It is ALSO the only gate on E4's headline metric. The drain instant (`MEASURED_DRAIN_NS`) and
/// the round count (`MEASURED_ROUNDS`) are asserted against this run, because neither is inside
/// the two frozen fingerprints — see the constants' own doc comments.
#[test]
#[ignore = "explicit P12 E4 count gates: full 4 s horizon, 104 GB of payload"]
fn e4_runs_to_completion_and_drains() {
    let image = lower(FIXTURE);
    // The ROUND path, not the plain one, because E4's own metric is the DRAIN INSTANT and
    // `RoundMetrics::frontier_ns` already carries it. Same canonical scalar execution, so the
    // `RunResult` below is the anchored one; the round vector is instrumentation the executor
    // has always returned (the F-HET horizon trace established that it needs no executor delta).
    let run = run_scalar_rounds_with_observations(&image, None, ObservationMode::Summary)
        .expect("scalar round run must succeed");
    let result = run.result;
    let drain_ns = run
        .rounds
        .last()
        .map(|round| round.frontier_ns)
        .expect("a run this long has rounds");
    println!(
        "E4 DRAIN: drainNs={drain_ns} horizonNs={HORIZON_NS} marginNs={} rounds={} \
         uncongestedDrainBoundNs={}",
        HORIZON_NS.saturating_sub(drain_ns),
        run.rounds.len(),
        LATEST_ARRIVAL_NS + LARGEST_FLOW_BYTES * 8 * 1_000_000_000 / PORT_RATE_BPS as u64,
    );

    // DIAGNOSE FIRST, ASSERT SECOND. A run this expensive must not fail on its first assertion
    // and throw away the state that explains the failure: the whole point of the gate is to say
    // WHICH invariant broke and by how much.
    let mut completed = 0_usize;
    let mut demanded = 0_u128;
    let mut emitted = 0_u128;
    let mut acked = 0_u128;
    let mut in_flight = 0_u128;
    let mut in_flight_flows = 0_usize;
    let mut armed_timers = 0_usize;
    let mut latest_deadline_ns = 0_u64;
    let mut stalled = Vec::new();
    for host in &result.host_states {
        for generator in &host.generators {
            if let days_executor::FlowGeneratorKind::Tcp(tcp) = generator.kind {
                demanded += u128::from(tcp.total_bytes);
                emitted += u128::from(generator.bytes_emitted);
                acked += u128::from(tcp.highest_ack);
                if tcp.highest_ack >= tcp.total_bytes {
                    completed += 1;
                } else {
                    // An incomplete flow is the only thing that can make the horizon binding, so
                    // it is reported in full: what it owes, and WHEN its transport intends to try
                    // again. That deadline is what sizes `duration`.
                    stalled.push((
                        generator.flow,
                        tcp.total_bytes,
                        tcp.highest_ack,
                        tcp.bytes_in_flight,
                        tcp.rto_ns,
                        tcp.srtt_ns,
                        tcp.active_timer.map(|timer| timer.deadline_ns),
                    ));
                }
                if tcp.bytes_in_flight != 0 {
                    in_flight += u128::from(tcp.bytes_in_flight);
                    in_flight_flows += 1;
                }
                if let Some(timer) = tcp.active_timer {
                    armed_timers += 1;
                    latest_deadline_ns = latest_deadline_ns.max(timer.deadline_ns);
                }
            }
        }
    }
    for entry in &stalled {
        println!(
            "E4 stalled flow {:?}: total={} acked={} inFlight={} rto_ns={} srtt_ns={} \
             timerDeadlineNs={:?}",
            entry.0, entry.1, entry.2, entry.3, entry.4, entry.5, entry.6
        );
    }
    println!("E4 latest armed retransmission deadline: {latest_deadline_ns} ns");

    println!(
        "E4 completion state: completed={completed}/{FLOW_COUNT} demandedBytes={demanded} \
         ackedBytes={acked} emittedBytes={emitted} flowsWithBytesInFlight={in_flight_flows} \
         bytesInFlight={in_flight} armedRetransmissionTimers={armed_timers} \
         residentPackets={} pendingEvents={}",
        result.resident_packets.len(),
        result.pending_events.len(),
    );
    // The retransmission publication: emitted payload beyond demand IS the retransmitted volume,
    // read straight off complete state with no instrumentation and no measurement delta.
    let retransmitted = emitted.saturating_sub(demanded);
    println!(
        "E4 retransmission publication: demanded={demanded} B emitted={emitted} B \
         retransmitted={retransmitted} B ({:.6}%) sourced={} received={} departed={} \
         dropped_packets={} dropped_bytes={}",
        100.0 * retransmitted as f64 / demanded as f64,
        result.summary.sourced_packets,
        result.summary.received_packets,
        result.summary.departed_packets,
        result.summary.dropped_packets,
        result.summary.dropped_bytes,
    );

    assert_eq!(demanded, TOTAL_PAYLOAD_BYTES);
    assert_eq!(completed, FLOW_COUNT, "8,192 of 8,192 flows must complete");
    assert_eq!(
        acked, TOTAL_PAYLOAD_BYTES,
        "every flow's cumulative ACK must reach its own GeDES-defined size, so the fabric \
         delivered exactly the byte total GeDES's table demands"
    );
    assert_eq!(
        in_flight, 0,
        "a completed flow has nothing in flight ({in_flight_flows} flows still did)"
    );
    assert_eq!(
        armed_timers, 0,
        "no retransmission timer may still be armed once every flow has completed"
    );
    assert!(
        result.resident_packets.is_empty(),
        "the fabric must be empty at the drain instant"
    );
    assert!(
        result.pending_events.is_empty(),
        "no event may remain scheduled once every flow has completed"
    );
    // G4, the Days-side twin of the ns-3 scenario's drain gate: the run ended because the fabric
    // drained, not because the clock ran out.
    assert!(
        drain_ns < HORIZON_NS,
        "the horizon was BINDING (drain {drain_ns} ns >= {HORIZON_NS} ns): E4 is a completion-time \
         fixture, not a fixed-window one"
    );

    // E4'S OWN METRIC, GATED — not merely printed. The drain instant and the round count are the
    // numbers E4 exists to produce and they are frozen in five artifacts (this file, the fixture
    // header, the generator, and evidence/P12/e4-authoring.md §1/§5/§6.2/§6.4/§9), yet NEITHER is
    // inside a fingerprint: `RunResult` and `RunSummary` carry no time field at all, and the drain
    // lives in `ScalarRoundRun::rounds`, which is instrumentation. Before this assertion existed,
    // a millisecond of executor TCP drift would have left all fifteen gates green while every one
    // of those artifacts silently went false. That is the vacuous-gate defect class T21 §4.7 had
    // to fix in this suite's siblings, applied to E4's headline number.
    assert_eq!(
        drain_ns, MEASURED_DRAIN_NS,
        "E4's DRAIN INSTANT moved. This is E4's own time-to-completion metric; if the change is \
         intended, the fixture header, the generator, this constant and \
         evidence/P12/e4-authoring.md must all move with it in the same commit"
    );
    assert_eq!(
        run.rounds.len(),
        MEASURED_ROUNDS,
        "E4's EXECUTOR ROUND COUNT moved; same obligation as the drain instant above"
    );
}

/// Metal refuses E4's run to completion by exhausting its TCP segment ledger, and it is pinned.
///
/// A first-class result, not a skipped backend. Metal executes this same image happily for
/// 1.152 ms — the prefix anchor is a four-backend fingerprint — so the fixture is expressible on
/// Metal; a 96 ms, 104 GB run is not. Verbatim, and reproduced twice on an isolated worktree:
///
/// > Metal execution failed after 16 capacity retries: Metal TCP segment ledger capacity of 98
/// > records exceeded at flow FlowId(3667); observed demand 99
///
/// Note the shape: the executor's own capacity-retry loop grew the ledger sixteen times and still
/// landed exactly ONE record short of the demand. That is a sizing-heuristic question for the
/// Metal lane, not a fixture question, and this round neither diagnosed it nor tuned around it —
/// `MetalConfig::default()` is what every other E4 gate uses and is what this one uses.
///
/// If Metal is ever fixed, THIS TEST GOES RED, which is the point: the exclusion in
/// `e4_completion_anchor_is_identical_across_local_backends` must be revisited in the same change
/// and E4's completion identity claim upgraded from three backends to four.
#[test]
#[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
#[ignore = "explicit P12 E4 Metal refusal probe: full 4 s horizon, 104 GB of payload"]
fn e4_completion_on_metal_exhausts_the_tcp_segment_ledger() {
    use days_executor::{MetalConfig, MetalExecutor};
    let image = lower(FIXTURE);
    let executor = MetalExecutor::new().expect("Metal executor must initialize");
    let outcome = executor.run_with_observations(
        &image,
        None,
        MetalConfig::default(),
        ObservationMode::Summary,
    );
    let error = match outcome {
        Ok(_) => panic!(
            "Metal now COMPLETES E4. That is good news and this test is the notice: re-run the \
             completion anchor with `identical_across_local_backends`, upgrade E4's completion \
             identity claim from three backends to four, and update evidence/P12/e4-authoring.md."
        ),
        Err(error) => error.to_string(),
    };
    // CLASS, not instance. This pins the error CLASS — a TCP-segment-ledger capacity exhaustion —
    // and deliberately not the three numbers that characterise today's instance (capacity 98,
    // demand 99, sixteen retries) or `FlowId(3667)`, because those are Metal-lane sizing details
    // that the Metal lane is expected to move. Anything that claims this gate "fails if the error
    // ever changes" is overstating it; it fails if the error CLASS changes, and it fails if Metal
    // ever succeeds. The verbatim string with the numbers is printed below and recorded in
    // evidence/P12/e4-authoring.md §6.4.
    assert!(
        error.contains("TCP segment ledger capacity"),
        "Metal still refuses E4 to completion, but with a DIFFERENT error CLASS than the one \
         recorded in evidence/P12/e4-authoring.md §6.4: {error}"
    );
    println!("E4 Metal completion refusal, verbatim: {error}");
}

fn assert_anchor(name: &str, actual: Fingerprint, bytes: u64, fnv1a64: u64) {
    assert_eq!(
        actual,
        Fingerprint { bytes, fnv1a64 },
        "{name}: frozen anchor moved (got bytes={} fnv1a64={:016x})",
        actual.bytes,
        actual.fnv1a64
    );
}

// -------------------------------------------------------------------------------------------
// Frozen anchor values. See `evidence/P12/e4-authoring.md` for how each was produced.
// -------------------------------------------------------------------------------------------

/// Scalar/CPU/Metal fingerprint of E4's first 1.152 ms, frozen by the T21 authoring round.
const E4_PREFIX_ANCHOR_BYTES: u64 = 114_896_891;
const E4_PREFIX_ANCHOR_FNV: u64 = 0x9d85_6e6d_66b0_17ae;

/// Scalar + CPU(2) + CPU(4) fingerprint of E4 run to completion. Metal refuses; see above.
///
/// Frozen from an ISOLATED worktree at this fixture's own commit, after another lane's uncommitted
/// executor work was found in the shared tree (`evidence/P12/e4-authoring.md` §6.4). The value is
/// bit-identical to the one the shared tree produced, which is the cross-check that the other
/// lane's changes were device-only — but the frozen number is the isolated one.
const E4_COMPLETION_ANCHOR_BYTES: u64 = 50_866_719;
const E4_COMPLETION_ANCHOR_FNV: u64 = 0x64fb_70c3_3345_c10e;

// -------------------------------------------------------------------------------------------
// md5, so the provenance gate does not need a dependency the workspace does not already carry.
// -------------------------------------------------------------------------------------------

fn md5_hex(message: &[u8]) -> String {
    const S: [u32; 64] = [
        7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 5, 9, 14, 20, 5, 9, 14, 20, 5,
        9, 14, 20, 5, 9, 14, 20, 4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23, 6, 10,
        15, 21, 6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15, 21,
    ];
    let k: [u32; 64] =
        std::array::from_fn(|index| ((index as f64 + 1.0).sin().abs() * 4_294_967_296.0) as u32);

    let mut state = [0x6745_2301_u32, 0xefcd_ab89, 0x98ba_dcfe, 0x1032_5476];
    let mut padded = message.to_vec();
    padded.push(0x80);
    while padded.len() % 64 != 56 {
        padded.push(0);
    }
    padded.extend_from_slice(&((message.len() as u64) * 8).to_le_bytes());

    for chunk in padded.chunks_exact(64) {
        let words: [u32; 16] = std::array::from_fn(|index| {
            u32::from_le_bytes(chunk[index * 4..index * 4 + 4].try_into().expect("word"))
        });
        let [mut a, mut b, mut c, mut d] = state;
        for index in 0..64 {
            let (mut f, g) = match index / 16 {
                0 => ((b & c) | (!b & d), index),
                1 => ((d & b) | (!d & c), (5 * index + 1) % 16),
                2 => (b ^ c ^ d, (3 * index + 5) % 16),
                _ => (c ^ (b | !d), (7 * index) % 16),
            };
            f = f
                .wrapping_add(a)
                .wrapping_add(k[index])
                .wrapping_add(words[g]);
            a = d;
            d = c;
            c = b;
            b = b.wrapping_add(f.rotate_left(S[index]));
        }
        state[0] = state[0].wrapping_add(a);
        state[1] = state[1].wrapping_add(b);
        state[2] = state[2].wrapping_add(c);
        state[3] = state[3].wrapping_add(d);
    }

    state
        .iter()
        .flat_map(|word| word.to_le_bytes())
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[cfg(test)]
mod md5_self_check {
    use super::md5_hex;

    /// RFC 1321 test vectors, so the provenance gate cannot pass on a broken digest.
    #[test]
    fn md5_matches_rfc1321_vectors() {
        assert_eq!(md5_hex(b""), "d41d8cd98f00b204e9800998ecf8427e");
        assert_eq!(md5_hex(b"abc"), "900150983cd24fb0d6963f7d28e17f72");
        assert_eq!(
            md5_hex(b"message digest"),
            "f96b697d7cb7938d525a2f31aaf161d0"
        );
        assert_eq!(
            md5_hex(b"abcdefghijklmnopqrstuvwxyz"),
            "c3fcd3d76192e4007dfb496cca67e13b"
        );
    }
}

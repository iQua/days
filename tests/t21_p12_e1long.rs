//! T21 (P12) E1-LONG fixture gates: contract, budget arithmetic, cross-backend identity, anchors.
//!
//! E1-LONG is the E1 family at GeDES's ACTUAL simulated span. GeDES's `--timeslots=20000` runs
//! advance 20,000 quanta of `TIMESLOT_LENGTH = 1,000 ns`, i.e. **20 ms**, not 20 us; GeDES prints
//! that correctly and the 20-us reading was our own arithmetic error. Matching that span is not a
//! matter of raising `duration`: E1 funds each flow with `size = 1_540_000` B = 1,000 packets,
//! which at the 10% point drains at 1.231 ms and at the 90% point at 0.137 ms, so an E1 file run
//! to 20 ms is the same traffic followed by a long empty fabric. E1-LONG therefore re-authors the
//! per-flow byte budget from the pacing arithmetic, and these gates pin that arithmetic rather
//! than the resulting constants alone.
//!
//! Three kinds of gate here, matching `t21_p12_fixtures.rs`:
//!
//! 1. **Contract** — E1-LONG differs from E1 in exactly `duration`, `size` and `log_path`, and its
//!    budget is the unique minimal-and-sufficient one for a 20 ms horizon.
//! 2. **Cross-backend identity** — scalar, CPU (two worker counts) and, on Apple hardware, Metal
//!    must return byte-identical `RunResult`s. CUDA is not reachable from this machine and is
//!    deferred, exactly as for E1.
//! 3. **Frozen anchor** — the scalar `RunResult` fingerprint recorded when the fixture was
//!    authored.
//!
//! The anchors carry `#[ignore]` and name the horizon each was frozen at, because two of the four
//! points are anchored at a PREFIX horizon rather than the fixture's own 20 ms; see the constant
//! table at the bottom of this file, which states which is which and why.

use std::fmt::{self, Debug, Write as _};
use std::path::PathBuf;

use days::scenario::compile_config;
use days_executor::{
    Backend, CpuConfig, ObservationMode, RunResult, SimulationImage, run_cpu_with_observations,
    run_scalar_with_observations, validate,
};

const FNV1A64_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
const FNV1A64_PRIME: u64 = 0x0000_0100_0000_01b3;

/// The E1-LONG horizon, in nanoseconds: GeDES's 20,000 timeslots x 1,000 ns per timeslot.
const HORIZON_NS: u64 = 20_000_000;
/// On-wire packet: 1,460 B payload + 80 B header, matched to GeDES exactly.
const WIRE_BYTES: u64 = 1_540;
/// Every host is a source.
const FLOW_COUNT: u64 = 8_192;

/// The four points, as `(load, fixture, inter-packet interval ns, per-flow packets, size bytes)`.
const POINTS: [(u32, &str, u64, u64, i64); 4] = [
    (
        10,
        "e1long_open_k32_load_10.toml",
        1_232,
        16_234,
        25_000_360,
    ),
    (30, "e1long_open_k32_load_30.toml", 411, 48_662, 74_939_480),
    (60, "e1long_open_k32_load_60.toml", 205, 97_561, 150_243_940),
    (
        90,
        "e1long_open_k32_load_90.toml",
        137,
        145_986,
        224_818_440,
    ),
];

/// The frozen E1 (20 us) counterpart of each E1-LONG point, and its frozen `sourced_packets`.
const E1_POINTS: [(&str, u64, u128); 4] = [
    ("e1_open_k32_load_10.toml", 1_232, 139_264),
    ("e1_open_k32_load_30.toml", 411, 401_408),
    ("e1_open_k32_load_60.toml", 205, 802_816),
    ("e1_open_k32_load_90.toml", 137, 1_196_032),
];

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

/// Number of emissions a horizon admits from a flow paced at a fixed integer interval.
///
/// A `PacketDistribution` flow emits at `t = 0, I, 2I, ...`, so the count is `floor(H/I) + 1`.
/// This is THE function that authored E1-LONG's byte budgets, so it is defined once here and
/// checked against the frozen E1 anchors before it is used to assert anything about E1-LONG.
fn emissions_admitted(horizon_ns: u64, interval_ns: u64) -> u64 {
    horizon_ns / interval_ns + 1
}

/// Runs the fixture on every backend this machine can reach and returns the shared result.
///
/// Scalar is the reference. CPU is run at two worker counts because the worker count is a host
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
// Contract.
// -------------------------------------------------------------------------------------------

/// The emission model that authored E1-LONG's budgets must reproduce E1's frozen anchors.
///
/// This is the load-bearing test of the whole family. `floor(H/I) + 1` is what sets every `size`
/// below; if it were wrong, the E1-LONG budgets would be wrong in a way no other assertion here
/// could see, because they would be self-consistent. At E1's 20,000 ns horizon the same function
/// must produce 17/49/98/146 packets per flow and therefore exactly the four `sourced_packets`
/// values frozen in `t21_p12_fixtures.rs`.
#[test]
fn the_emission_model_reproduces_the_frozen_e1_sourced_anchors() {
    for (name, interval_ns, frozen_sourced) in E1_POINTS {
        let per_flow = emissions_admitted(20_000, interval_ns);
        assert_eq!(
            u128::from(per_flow) * u128::from(FLOW_COUNT),
            frozen_sourced,
            "{name}: floor(20000/{interval_ns}) + 1 = {per_flow} per flow does not reproduce the \
             frozen anchor; the model that authored E1-LONG's budgets is wrong"
        );
    }
}

/// E1-LONG differs from E1 in EXACTLY the horizon, the byte budget and the log path.
///
/// Everything a cross-arm claim rests on -- fabric, matrix, routing policy, wire packet, interval,
/// seed, flow count -- must be E1's, byte for byte in the parsed table. If a later edit moves any
/// of it, E1-LONG stops being "E1 at GeDES's span" and the pairing of the two families in a paper
/// row stops meaning what it says.
#[test]
fn e1long_is_e1_with_only_the_horizon_the_budget_and_the_log_path_changed() {
    for (load, long_name, _, _, _) in POINTS {
        let short_name = format!("e1_open_k32_load_{load:02}.toml");
        let short = fixture_table(&short_name);
        let long = fixture_table(long_name);

        for key in ["topology", "switch", "link", "routing", "seed", "threading"] {
            assert_eq!(
                short.get(key),
                long.get(key),
                "{long_name}: must share E1's {key}"
            );
        }
        assert_eq!(
            short.keys().collect::<Vec<_>>(),
            long.keys().collect::<Vec<_>>(),
            "{long_name}: must not add or drop a top-level key"
        );

        let short_set = &short["flow_set"][0];
        let long_set = &long["flow_set"][0];
        for key in ["flow_type", "flow_count", "pairing"] {
            assert_eq!(
                short_set.get(key),
                long_set.get(key),
                "{long_name}: must share E1's flow_set {key}"
            );
        }
        for key in ["initial_delay", "arr_dist", "pkt_size_dist"] {
            assert_eq!(
                short_set["traffic"].get(key),
                long_set["traffic"].get(key),
                "{long_name}: must share E1's traffic {key} -- the interval especially, since the \
                 achieved offered load is a property of it alone"
            );
        }

        // ...and the two that MUST differ, together. Raising one without the other is exactly the
        // defect E1-LONG exists to fix.
        assert_ne!(short["duration"], long["duration"]);
        assert_ne!(short_set["traffic"]["size"], long_set["traffic"]["size"]);
        assert_eq!(
            long["log_path"].as_str(),
            Some(format!("logs/p12/e1long_open_k32_load_{load:02}").as_str())
        );
    }
}

/// The horizon is GeDES's 20,000 timeslots, stated in the unit GeDES actually uses.
///
/// One GeDES timeslot is `TIMESLOT_LENGTH = 1,000 ns`, so 20,000 of them are 20 ms. The frozen E1
/// family's 20 us horizon is 20 such timeslots, not 20,000; that mislabelling is corrected at
/// source in E1's own headers, and this assertion is the machine-checked form of the correction.
#[test]
fn e1long_horizon_is_gedes_twenty_thousand_thousand_nanosecond_timeslots() {
    const GEDES_TIMESLOTS: u64 = 20_000;
    const GEDES_TIMESLOT_NS: u64 = 1_000;
    assert_eq!(GEDES_TIMESLOTS * GEDES_TIMESLOT_NS, HORIZON_NS);
    for (_, name, _, _, _) in POINTS {
        let config = fixture_table(name);
        assert_eq!(
            config["duration"].as_float(),
            Some(HORIZON_NS as f64 / 1e9),
            "{name}: horizon must be GeDES's 20,000 x 1,000 ns span, i.e. 20 ms"
        );
    }
}

/// Each per-flow byte budget is the UNIQUE minimal-and-sufficient one for the 20 ms horizon.
///
/// Sufficient: the last emission the horizon admits is funded, so traffic persists to within one
/// interval of the horizon instead of draining at 0.137-1.231 ms as the frozen E1 files do.
/// Minimal: one more packet would be funded past the horizon and would never be emitted, so the
/// budget states the workload rather than merely over-providing for it. Together the two
/// inequalities admit exactly one N, which is why this test can assert the constants and the
/// arithmetic that produced them at the same time.
#[test]
fn e1long_budgets_are_minimal_and_sufficient_for_the_twenty_millisecond_horizon() {
    for (_, name, interval_ns, per_flow, size) in POINTS {
        let config = fixture_table(name);
        let traffic = &config["flow_set"][0]["traffic"];
        assert_eq!(
            traffic["arr_dist"]["low"].as_float(),
            Some(interval_ns as f64 / 1e9),
            "{name}: interval"
        );
        assert_eq!(traffic["arr_dist"]["low"], traffic["arr_dist"]["high"]);
        assert_eq!(
            traffic["pkt_size_dist"]["low"].as_integer(),
            Some(WIRE_BYTES as i64),
            "{name}: 1,460 B payload + 80 B header, matched to GeDES on the wire"
        );
        assert_eq!(traffic["size"].as_integer(), Some(size), "{name}: budget");

        // The budget must be a whole number of wire packets, or the last packet is a runt and the
        // offered-load arithmetic stops holding.
        let budget = size as u64;
        assert_eq!(
            budget % WIRE_BYTES,
            0,
            "{name}: budget must be whole packets"
        );
        assert_eq!(budget / WIRE_BYTES, per_flow, "{name}: packets per flow");

        assert_eq!(
            per_flow,
            emissions_admitted(HORIZON_NS, interval_ns),
            "{name}: budget must be floor(H/I) + 1 packets"
        );
        assert!(
            (per_flow - 1) * interval_ns <= HORIZON_NS,
            "{name}: SUFFICIENT fails -- the last funded emission at {} ns is past the horizon",
            (per_flow - 1) * interval_ns
        );
        assert!(
            per_flow * interval_ns > HORIZON_NS,
            "{name}: MINIMAL fails -- packet {} would be funded at {} ns, inside the horizon, so \
             the budget is not the tightest one that spans it",
            per_flow + 1,
            per_flow * interval_ns
        );
    }
}

/// Traffic must persist essentially to the horizon -- that IS the fixture family's reason to exist.
///
/// The defect being fixed is quantified rather than asserted qualitatively: E1's 1,000-packet
/// budget stops emitting at 1.231 ms (load 10) to 0.137 ms (load 90), i.e. after 0.7-6.2 % of a
/// 20 ms span, and the rest of such a run is a drained fabric. E1-LONG must exceed 99.9 %.
#[test]
fn e1long_traffic_persists_the_full_span_where_e1s_budget_drains_early() {
    const E1_PACKETS_PER_FLOW: u64 = 1_540_000 / WIRE_BYTES;
    for (_, name, interval_ns, per_flow, _) in POINTS {
        let e1_last = (E1_PACKETS_PER_FLOW - 1) * interval_ns;
        let long_last = (per_flow - 1) * interval_ns;
        assert!(
            e1_last * 100 < HORIZON_NS * 7,
            "{name}: E1's budget was expected to drain inside 7% of the span, got {e1_last} ns"
        );
        assert!(
            long_last * 1_000 >= HORIZON_NS * 999,
            "{name}: E1-LONG traffic must persist past 99.9% of the span, got {long_last} ns"
        );
    }
}

/// The achieved offered load of each point, computed the way an external arm computes its own.
///
/// Identical to E1's, because the interval is identical -- which is the point: E1-LONG changes the
/// SPAN, not the LOAD, so the two families are one sweep in load and a pair in span.
#[test]
fn e1long_achieved_offered_load_matches_e1s_disclosed_values() {
    const DISCLOSED: [f64; 4] = [10.000_000_0, 29.975_669_1, 60.097_561_0, 89.927_007_3];
    for ((load, name, interval_ns, _, _), disclosed) in POINTS.into_iter().zip(DISCLOSED) {
        // 1,540 B packet, 100 Gbit/s host: load = size * 8 / (interval_ns * 100).
        let achieved = WIRE_BYTES as f64 * 8.0 / (interval_ns as f64 * 100.0) * 100.0;
        assert!(
            (achieved - disclosed).abs() < 1e-6,
            "{name}: achieved {achieved} disagrees with the disclosed {disclosed}"
        );
        assert!(
            (achieved - f64::from(load)).abs() <= 0.10,
            "{name}: achieved {achieved} is further than the disclosed 0.098 pp from nominal"
        );
    }
}

/// Sourced-packet totals, stated as a fixture property rather than left to a run to discover.
///
/// These are the numbers a cross-arm table compares against GeDES F4's `tx_packets`, so they are
/// pinned here and re-asserted against the actual run in the anchors below.
#[test]
fn e1long_sourced_packet_totals_are_the_pinned_cross_arm_quantities() {
    const EXPECTED: [u128; 4] = [132_988_928, 398_639_104, 799_219_712, 1_195_917_312];
    // GeDES F4's own tx_packets at the same four loads and the same 20 ms span.
    const GEDES_TX: [u128; 4] = [132_980_736, 398_958_592, 797_917_184, 1_196_875_776];
    for (((_, name, _, per_flow, _), expected), gedes) in
        POINTS.into_iter().zip(EXPECTED).zip(GEDES_TX)
    {
        assert_eq!(u128::from(per_flow) * u128::from(FLOW_COUNT), expected);
        // The gap against GeDES must be no worse than the disclosed interval rounding (0.163%).
        let relative = (expected as f64 - gedes as f64).abs() / gedes as f64;
        assert!(
            relative < 0.002,
            "{name}: {expected} sourced against GeDES's {gedes} is {relative} relative, which is \
             more than the disclosed interval-rounding deviation can explain"
        );
    }
}

// -------------------------------------------------------------------------------------------
// Static ECMP feasibility — why three of the four points cannot be drop-free at any long span.
// -------------------------------------------------------------------------------------------

/// Number of flows sharing the busiest link, and how many links carry more than they can serve.
///
/// A pure function of the lowered image, so it is exact rather than statistical.
fn ecmp_link_pressure(name: &str, interval_ns: u64) -> (u64, u64, usize) {
    let image = lower(name);
    let mut per_link = std::collections::BTreeMap::<u64, u64>::new();
    for flow in &image.flows {
        for link in &flow.route {
            *per_link.entry(link.0).or_default() += 1;
        }
    }
    // A 100 Gbit/s link serialises a 1,540 B packet in 123.2 ns, and each flow offers one packet
    // per `interval_ns`, so a link sustains `floor(interval_ns / 123.2)` flows. The comparison is
    // done in integers -- 10 x interval against 1,232 x flows -- because the fixture's own
    // arithmetic is integer and a float here would decide the 60% point by rounding.
    let sustainable = (10 * interval_ns) / 1_232;
    let overloaded = per_link
        .values()
        .filter(|count| **count > sustainable)
        .count();
    let busiest = *per_link
        .values()
        .max()
        .unwrap_or_else(|| panic!("{name}: no routed links"));
    (busiest, sustainable, overloaded)
}

/// **E1's drop-freeness is a property of its 20 us horizon, not of its fabric.** FINDING, pinned.
///
/// `FatTreeEcmp` selects one aggregation group and one core offset per flow from disjoint halves
/// of the flow hash (`src/topos/route.rs`), so the 16 flows leaving an edge switch land on its 16
/// uplinks as balls in bins, not one per uplink. The busiest link on this matrix carries **6 or 7**
/// flows at every load. A 100 Gbit/s link serialises a 1,540 B packet in 123.2 ns, so it sustains
/// `floor(I / 123.2)` flows -- 10 at load 0.10, but only 3 at 0.30 and **1** at 0.60 and 0.90.
///
/// The consequence is not a transient: a link offered more than it can serve queues without bound
/// and drops once `switch.capacity` fills, at any queue depth, for as long as traffic lasts. At
/// E1's 20 us horizon the fabric is still filling and the 1,024-packet queues have not yet
/// overflowed, which is why all four frozen E1 anchors are drop-free. Extend the span and the
/// overload becomes visible: measured on E1-LONG at a 200 us prefix, scalar drops
/// **0 / 2,506 / 440,564 / 2,200,132** packets at loads 0.10 / 0.30 / 0.60 / 0.90 -- exactly the
/// pattern this test predicts from the image alone, with no run at all.
///
/// This matters for the cross-arm claim, which is why it is a gate and not a comment. GeDES F4
/// delivers **0.999692 / 0.999651 / 0.999641 / 0.999630** over the same 20 ms span at the same
/// four loads, with the whole shortfall pipeline occupancy at the horizon rather than loss. So at
/// loads 0.30 and above E1-LONG and GeDES are no longer running the same workload, and only the
/// load-0.10 point is a drop-free long-span row today. Closing the other three needs a
/// **balanced** fat-tree path assignment (one uplink per source ordinal rather than a hash), which
/// is a routing-policy change in `src/topos/route.rs` with its own gates -- out of scope for a
/// fixture-authoring round, and recorded rather than attempted.
#[test]
fn static_ecmp_cannot_sustain_this_matrix_above_the_ten_percent_point() {
    const EXPECTED: [(u64, u64, usize); 4] = [
        // (busiest link, flows it can sustain, links offered more than they can serve)
        (6, 10, 0),
        (7, 3, 592),
        (7, 1, 8_608),
        (7, 1, 8_640),
    ];
    for ((load, name, interval_ns, _, _), expected) in POINTS.into_iter().zip(EXPECTED) {
        let actual = ecmp_link_pressure(name, interval_ns);
        assert_eq!(
            actual, expected,
            "{name}: hash-ECMP link pressure moved (busiest, sustainable, overloaded) = {actual:?}"
        );
        let (busiest, sustainable, overloaded) = actual;
        assert_eq!(
            overloaded == 0,
            busiest <= sustainable,
            "{name}: the busiest link and the overloaded-link count must agree"
        );
        if load == 10 {
            assert_eq!(
                overloaded, 0,
                "{name}: the 10% point is the one long-span row whose fabric is not the bottleneck"
            );
        } else {
            assert!(
                overloaded > 0,
                "{name}: this point is EXPECTED to overload under hash ECMP; if it no longer does, \
                 the routing policy changed and the E1-LONG drop disclosure must be revisited"
            );
        }
    }
}

// -------------------------------------------------------------------------------------------
// Anchors.
// -------------------------------------------------------------------------------------------

/// Frozen scalar anchors, cross-backend identical, one test per point.
///
/// One test per point rather than a loop over four, because these are the most expensive gates in
/// the repository and a failure at one load must not hide the state of the other three.
///
/// `$dropped` is asserted rather than assumed zero. Three of the four points DO drop, for the
/// structural reason the feasibility gate above states, and an anchor that pretended otherwise
/// would be the wrong shape of gate: what has to be frozen is the drop count, so that a change in
/// routing, queueing or pacing moves it and is seen.
macro_rules! e1long_anchor {
    (
        $test:ident, $fixture:literal, $horizon:expr,
        $sourced:literal, $dropped:literal, $bytes:literal, $fnv:literal
    ) => {
        #[test]
        #[ignore = "explicit P12 E1-LONG anchor: see ANCHOR HORIZONS below"]
        fn $test() {
            let image = lower($fixture);
            let horizon: Option<u64> = $horizon;
            let scalar = scalar_run(&image, horizon);
            assert_eq!(scalar.summary.sourced_packets, $sourced, "{} sourced", $fixture);
            assert_eq!(
                scalar.summary.dropped_packets, $dropped,
                "{}: frozen drop count moved -- E1-LONG's loss is a structural property of static \
                 ECMP on this matrix (see static_ecmp_cannot_sustain_this_matrix_above_the_ten_\
                 percent_point), so a move here is a semantics change, not noise",
                $fixture
            );
            assert_anchor(
                $fixture,
                identical_across_local_backends($fixture, &image, horizon),
                $bytes,
                $fnv,
            );
        }
    };
}

// ANCHOR HORIZONS, and why they are not all the same.
//
// Load 0.10 is anchored at the fixture's OWN 20 ms horizon. It is the one point whose fabric is
// not the bottleneck at a long span (0 overloaded links), so its 20 ms image is the E1-LONG row a
// cross-arm table can actually use, and it is frozen in full.
//
// Loads 0.30, 0.60 and 0.90 are anchored at a 200 us PREFIX horizon. This is a disclosed decision
// with two independent reasons, and neither is "it was slow":
//
//   1. SEMANTICS. Those three points overload 592 / 8,608 / 8,640 links under static ECMP and
//      lose 0.06 % / 5.5 % / 18.4 % of sourced packets by 200 us alone, with the loss still
//      growing. A frozen 20 ms image of a saturated fabric would pin a workload that is NOT the
//      one GeDES runs (delivery 0.99963 at load 0.90) and that no cross-arm row should cite.
//      Freezing it would give that image an authority it must not have.
//   2. COST, stated because it is real: at 200 us the load-0.90 scalar run already carries
//      3.64 M resident packets and renders a 1.3 GB complete state; the 20 ms image would be a
//      queue-saturated fabric an order of magnitude larger, on every backend at once.
//
// 200 us is not an arbitrary prefix. It is 10x E1's whole horizon and ~23x this fabric's 8.6 us
// round trip, so the filling transient is over and the overload is already the dominant behaviour
// -- the prefix anchors DO carry the loss (2,506 / 440,564 / 2,200,132), they simply do not claim
// to be the 20 ms end state. What they pin is what an anchor is for: lowering, the re-authored
// budget (no generator is within 47x of exhaustion at 200 us), and scalar/CPU/Metal byte-identity.
//
// CUDA is not reachable from this machine and is deferred, exactly as for the frozen E1 family.

e1long_anchor!(
    e1long_load10_anchor_is_drop_free_and_identical_across_local_backends,
    "e1long_open_k32_load_10.toml",
    None,
    132_988_928,
    0,
    70_900_544,
    0x9f9e_8d13_e311_1ff2
);
e1long_anchor!(
    e1long_load30_prefix_anchor_is_identical_across_local_backends,
    "e1long_open_k32_load_30.toml",
    Some(200_000),
    3_989_504,
    2_506,
    172_686_274,
    0x642e_36cd_58f8_f5bb
);
e1long_anchor!(
    e1long_load60_prefix_anchor_is_identical_across_local_backends,
    "e1long_open_k32_load_60.toml",
    Some(200_000),
    7_995_392,
    440_564,
    776_891_569,
    0xa605_defe_7f00_8d65
);
e1long_anchor!(
    e1long_load90_prefix_anchor_is_identical_across_local_backends,
    "e1long_open_k32_load_90.toml",
    Some(200_000),
    11_960_320,
    2_200_132,
    1_313_022_440,
    0xa3cf_8acc_e34c_0780
);

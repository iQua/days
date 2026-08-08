//! T21 (P12 wave-4) fixture gates: contract, cross-backend identity, frozen anchors.
//!
//! Every fixture in `configs/benchmarks/p12/` is pinned three ways here:
//!
//! 1. **Contract** — the parameters a cross-arm or showcase claim rests on (packet size, load,
//!    matrix, tiers, thresholds) are asserted against the file, so an edit that changes what the
//!    fixture MEANS fails a test instead of silently changing a paper row.
//! 2. **Cross-backend identity** — scalar, CPU and (on Apple hardware) Metal must return
//!    byte-identical `RunResult`s. CUDA is not reachable from this machine and is deferred to the
//!    next measurer round; that deferral is stated, not implied.
//! 3. **Frozen anchor** — the scalar `RunResult` fingerprint recorded when the fixture was
//!    authored, so any later change to lowering or semantics that moves these images is visible.
//!
//! The fingerprint is FNV-1a64 over the pretty `Debug` rendering of the whole `RunResult`, the
//! same function `src/bin/t20f_frontier.rs` prints, so a fingerprint here and a fingerprint from
//! that binary are directly comparable.
//!
//! Heavy fixtures carry `#[ignore]`; the anchor horizon each one was frozen at is named on it.

use std::fmt::{self, Debug, Write as _};
use std::path::PathBuf;

use days::scenario::compile_config;
use days_executor::{
    Backend, CpuConfig, ObservationMode, RunResult, SimulationImage, run_cpu_with_observations,
    run_scalar_with_observations, validate,
};

const FNV1A64_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
const FNV1A64_PRIME: u64 = 0x0000_0100_0000_01b3;

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

/// Runs the fixture on every backend this machine can reach and returns the shared result.
///
/// Scalar is the reference. CPU is run at two different worker counts because the worker count is
/// a host scheduling parameter and must be invisible in complete state. Metal joins on Apple
/// hardware. CUDA is NOT reachable here; the four-backend claim for these fixtures is closed by
/// the next measurer round on boston/madrid, and is not asserted by this test.
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
// E1 — open-loop k=32 fat tree, 10/30/60/90% nominal offered host load.
// -------------------------------------------------------------------------------------------

/// The four E1 points must differ in EXACTLY one parameter: the inter-packet interval.
///
/// Everything else -- fabric, matrix, routing policy, packet size, per-flow byte budget, horizon,
/// seed -- is the swept-fixture contract. If a later edit moves any of it, the four points stop
/// being one sweep and the E1 row stops meaning what it says.
#[test]
fn e1_points_differ_only_in_the_inter_packet_interval() {
    const POINTS: [(&str, f64); 4] = [
        ("e1_open_k32_load_10.toml", 0.000_001_232),
        ("e1_open_k32_load_30.toml", 0.000_000_411),
        ("e1_open_k32_load_60.toml", 0.000_000_205),
        ("e1_open_k32_load_90.toml", 0.000_000_137),
    ];
    for (name, interval) in POINTS {
        let config = fixture_table(name);
        assert_eq!(config["seed"].as_integer(), Some(21_032), "{name} seed");
        assert_eq!(
            config["duration"].as_float(),
            Some(0.000_020),
            "{name} horizon must be GeDES's 20,000 x 1,000 ns timeslot window"
        );
        assert_eq!(config["topology"]["fat_tree"]["k"].as_integer(), Some(32));
        assert_eq!(
            config["topology"]["fat_tree"]["hosts_per_edge"].as_integer(),
            Some(16)
        );
        assert_eq!(
            config["switch"]["port_rate"].as_integer(),
            Some(100_000_000_000)
        );
        assert_eq!(config["switch"]["capacity"].as_integer(), Some(1_024));
        assert_eq!(config["link"]["propagation_ns"].as_integer(), Some(1_000));
        assert_eq!(
            config["routing"]["policy"].as_str(),
            Some("FatTreeEcmp"),
            "{name}: single-path routing funnels every cross-pod flow through one core switch"
        );

        let flow_set = &config["flow_set"][0];
        assert_eq!(
            flow_set["flow_count"].as_integer(),
            Some(8_192),
            "{name}: every host must be a source, as in GeDES's UDP mode"
        );
        assert_eq!(flow_set["pairing"].as_str(), Some("SwitchOffsetHalf"));
        let traffic = &flow_set["traffic"];
        assert_eq!(
            traffic["pkt_size_dist"]["low"].as_integer(),
            Some(1_540),
            "{name}: 1,460 B payload + 80 B header, matched to GeDES on the wire"
        );
        assert_eq!(traffic["size"].as_integer(), Some(1_540_000));
        assert_eq!(
            traffic["arr_dist"]["low"].as_float(),
            Some(interval),
            "{name} interval"
        );
        assert_eq!(traffic["arr_dist"]["low"], traffic["arr_dist"]["high"]);
    }
}

/// The achieved offered load of each point, computed from the fixture the way an external arm
/// computes its own: sourced wire bytes over host-seconds of line rate.
#[test]
fn e1_achieved_offered_load_matches_the_disclosed_values() {
    const EXPECTED: [(&str, u64, f64); 4] = [
        ("e1_open_k32_load_10.toml", 1_232, 10.000_000_0),
        ("e1_open_k32_load_30.toml", 411, 29.975_669_1),
        ("e1_open_k32_load_60.toml", 205, 60.097_561_0),
        ("e1_open_k32_load_90.toml", 137, 89.927_007_3),
    ];
    for (name, interval_ns, disclosed) in EXPECTED {
        // 1,540 B packet, 100 Gbit/s host: load = size * 8 / (interval_ns * 100).
        let achieved = 1_540.0 * 8.0 / (interval_ns as f64 * 100.0) * 100.0;
        assert!(
            (achieved - disclosed).abs() < 1e-6,
            "{name}: achieved {achieved} disagrees with the disclosed {disclosed}"
        );
        assert!(
            (achieved - nominal(name)).abs() <= 0.17,
            "{name}: achieved {achieved} is further than the disclosed 0.17 pp from nominal"
        );
    }
}

fn nominal(name: &str) -> f64 {
    name.trim_start_matches("e1_open_k32_load_")
        .trim_end_matches(".toml")
        .parse()
        .expect("E1 fixture names carry their nominal load")
}

/// E1's whole point is that the fabric is not the bottleneck, exactly as GeDES reports for the
/// same matrix. A dropped packet here means the arms are no longer running the same workload.
#[test]
#[ignore = "explicit P12 E1 anchor: full 20 us horizon on all four points"]
fn e1_anchors_are_drop_free_and_identical_across_local_backends() {
    const ANCHORS: [(&str, u64, u64, u128); 4] = [
        (
            "e1_open_k32_load_10.toml",
            70_809_309,
            0x951c_ad2c_3d9f_39f8,
            139_264,
        ),
        (
            "e1_open_k32_load_30.toml",
            131_534_072,
            0x2dbb_d522_d243_3b86,
            401_408,
        ),
        (
            "e1_open_k32_load_60.toml",
            256_696_554,
            0xb1ba_5a9d_872d_abbc,
            802_816,
        ),
        (
            "e1_open_k32_load_90.toml",
            379_505_175,
            0x8ae1_9e3f_4c91_b029,
            1_196_032,
        ),
    ];
    for (name, bytes, fnv1a64, sourced) in ANCHORS {
        let image = lower(name);
        let scalar = scalar_run(&image, None);
        assert_eq!(scalar.summary.sourced_packets, sourced, "{name} sourced");
        assert_eq!(
            scalar.summary.dropped_packets, 0,
            "{name}: E1 must be drop-free; GeDES reports no queue overflow on this matrix"
        );
        assert_anchor(
            name,
            identical_across_local_backends(name, &image, None),
            bytes,
            fnv1a64,
        );
    }
}

// -------------------------------------------------------------------------------------------
// E2 — closed-loop k=32 TCP Reno, E1's twin.
// -------------------------------------------------------------------------------------------

#[test]
fn e2_is_e1s_fabric_and_matrix_with_only_the_traffic_model_changed() {
    let e1 = fixture_table("e1_open_k32_load_90.toml");
    let e2 = fixture_table("e2_closed_k32_tcp_reno.toml");
    for key in ["topology", "switch", "link", "routing"] {
        assert_eq!(e1[key], e2[key], "E2 must share E1's {key}");
    }
    assert_eq!(e2["seed"], e1["seed"]);
    assert_eq!(
        e2["duration"].as_float(),
        Some(0.001_152),
        "E2 needs the standing k=32 closed-loop horizon: 20 us is under two round trips"
    );
    let flow_set = &e2["flow_set"][0];
    assert_eq!(flow_set["flow_type"].as_str(), Some("TCP"));
    assert_eq!(flow_set["flow_count"].as_integer(), Some(8_192));
    assert_eq!(flow_set["pairing"].as_str(), Some("SwitchOffsetHalf"));
    assert_eq!(
        flow_set["traffic"]["pkt_size_dist"]["low"].as_integer(),
        Some(1_460),
        "GeDES's as-shipped TCP payload, and E1's wire packet minus the same 80 B header"
    );
    assert_eq!(
        flow_set["traffic"]["size"].as_integer(),
        Some(16_777_216),
        "a 100 Gbit/s host can place at most 14.4 MB in 1.152 ms, so no flow may complete"
    );
}

#[test]
#[ignore = "explicit P12 E2 anchor: 50 us probe horizon, not the fixture's 1.152 ms"]
fn e2_anchor_is_identical_across_local_backends() {
    let name = "e2_closed_k32_tcp_reno.toml";
    let image = lower(name);
    assert_anchor(
        name,
        identical_across_local_backends(name, &image, Some(50_000)),
        E2_ANCHOR_BYTES,
        E2_ANCHOR_FNV,
    );
}

// -------------------------------------------------------------------------------------------
// F-HET, F-BURST, F-TOPO, AQM — the wave-4 showcase fixtures.
// -------------------------------------------------------------------------------------------

#[test]
fn f_het_carries_three_delay_tiers_a_hundred_fold_apart_in_one_image() {
    let name = "f_het_k8_tiered_delays.toml";
    let config = fixture_table(name);
    let tiers = &config["link"]["propagation_tiers"];
    assert_eq!(tiers["host_to_edge_ns"].as_integer(), Some(100));
    assert_eq!(tiers["edge_to_aggregation_ns"].as_integer(), Some(1_000));
    assert_eq!(tiers["aggregation_to_core_ns"].as_integer(), Some(10_000));
    assert!(
        config["link"].get("propagation_ns").is_none(),
        "a tiered fixture must not also declare a uniform delay"
    );

    let image = lower(name);
    let mut delays = image
        .links
        .iter()
        .map(|link| link.propagation_ns)
        .collect::<Vec<_>>();
    delays.sort_unstable();
    delays.dedup();
    assert_eq!(
        delays,
        vec![100, 1_000, 10_000],
        "all three tiers must be present in the one image"
    );

    // The rack-local phase must fall silent strictly before the horizon, and strictly before the
    // inter-pod phase does: that gap is the whole showcase.
    let rack = &config["flow_set"][0];
    let inter_pod = &config["flow_set"][1];
    assert_eq!(rack["pairing"].as_str(), Some("SameSwitchNext"));
    assert_eq!(inter_pod["pairing"].as_str(), Some("SwitchOffsetHalf"));
    let last_emission = |set: &toml::Value, packet_bytes: u64| {
        let size = set["traffic"]["size"].as_integer().unwrap() as u64;
        let interval = set["traffic"]["arr_dist"]["low"].as_float().unwrap();
        (size.div_ceil(packet_bytes) - 1) as f64 * interval
    };
    let rack_end = last_emission(rack, 1_540);
    let inter_pod_end = last_emission(inter_pod, 1_540);
    let horizon = config["duration"].as_float().unwrap();
    assert!(
        rack_end < inter_pod_end && inter_pod_end < horizon,
        "rack phase must end ({rack_end}) before the inter-pod phase ({inter_pod_end}) and both \
         before the horizon ({horizon})"
    );
}

#[test]
fn f_burst_phases_are_byte_terminated_coflows_that_leave_room_for_a_silence() {
    let name = "f_burst_coflow_phases.toml";
    let config = fixture_table(name);
    assert!(
        config.get("collective").is_none(),
        "{name}: a collective phrasing is refused on the device backends, so this fixture must \
         stay on a generator every backend supports"
    );
    let phases = config["flow_set"].as_array().expect("four coflow phases");
    assert_eq!(phases.len(), 4);
    let mut delays = Vec::new();
    for phase in phases {
        assert_eq!(phase["flow_type"].as_str(), Some("TCP"));
        assert_eq!(phase["flow_count"].as_integer(), Some(16));
        assert_eq!(phase["pairing"].as_str(), Some("SwitchOffsetHalf"));
        assert_eq!(phase["traffic"]["size"].as_integer(), Some(146_000));
        assert_eq!(
            phase["traffic"]["tcp"]["cc_algorithm"].as_str(),
            Some("TCPReno"),
            "{name}: the closed loop IS the protocol depth this fixture claims"
        );
        delays.push(phase["traffic"]["initial_delay"].as_float().unwrap());
    }
    assert_eq!(delays, vec![0.0, 0.000_250, 0.000_500, 0.000_750]);
    assert_eq!(config["duration"].as_float(), Some(0.001_000));

    // The silences are only real if every phase drains before the next one starts. Nothing
    // resident and nothing pending at the horizon is the evidence.
    let image = lower(name);
    assert_eq!(image.flows.len(), 4 * 16);
    let result = scalar_run(&image, None);
    assert_eq!(result.pending_events.len(), 0, "{name}: fabric must drain");
    assert_eq!(
        result.resident_packets.len(),
        0,
        "{name}: fabric must drain"
    );
    assert_eq!(result.summary.dropped_packets, 0);
}

#[test]
fn f_topo_is_a_balanced_dragonfly_no_fat_tree_arithmetic_can_address() {
    let name = "f_topo_dragonfly_g33.toml";
    let config = fixture_table(name);
    let dragonfly = &config["topology"]["dragonfly"];
    assert_eq!(dragonfly["routers_per_group"].as_integer(), Some(8));
    assert_eq!(dragonfly["global_ports_per_router"].as_integer(), Some(4));
    assert_eq!(dragonfly["hosts_per_router"].as_integer(), Some(4));
    assert!(
        config.get("routing").is_none(),
        "minimal dragonfly routing has no equal-cost set to spread over"
    );

    let image = lower(name);
    // g = a*h + 1 = 33 groups, 264 routers, 1,056 hosts.
    // Undirected fabric links: 33 * C(8,2) intra-group + C(33,2) global.
    let fabric_links = 33 * 28 + 33 * 32 / 2;
    assert_eq!(image.links.len(), 2 * fabric_links + 2 * 1_056);
    assert_eq!(image.flows.len(), 1_056);
}

#[test]
fn aqm_fixture_marks_below_a_tight_threshold_under_a_32_to_1_incast() {
    let name = "f_aqm_alias_incast32.toml";
    let config = fixture_table(name);
    assert_eq!(config["switch"]["drop"].as_str(), Some("ECN_THRESHOLD"));
    assert_eq!(config["switch"]["capacity"].as_integer(), Some(64));
    assert_eq!(config["switch"]["ecn_threshold"].as_float(), Some(0.125));
    assert_eq!(
        config["routing"]["policy"].as_str(),
        Some("FatTreeEcmp"),
        "without it the senders would also pile onto one core switch and the fixture would no \
         longer isolate the receiver-port AQM"
    );
    let flows = config["flow"].as_array().expect("explicit incast flows");
    assert_eq!(flows.len(), 32);
    for (index, flow) in flows.iter().enumerate() {
        assert_eq!(
            flow["flow_type"].as_str(),
            Some("PacketDistribution"),
            "{name}: no ECN-reacting protocol reaches all four backends, so this fixture stays \
             open-loop and keeps the marking decision itself as the observable"
        );
        let graph = flow["graph"].as_array().unwrap()[0].as_array().unwrap();
        assert_eq!(graph[0].as_integer(), Some(index as i64 + 1));
        assert_eq!(
            graph[1].as_integer(),
            Some(0),
            "every sender targets host 0"
        );
    }

    // The threshold has to be crossed, or there is no marking decision to alias. Marks live on
    // packets that are still in the fabric at the horizon and on packets queued in the switches.
    let image = lower(name);
    assert_eq!(image.flows.len(), 32);
    let result = scalar_run(&image, None);
    let marked_resident = result
        .resident_packets
        .iter()
        .filter(|packet| packet.ecn_marked)
        .count();
    assert!(
        marked_resident > 0,
        "{name}: no packet crossed the ECN threshold, so there is nothing to alias"
    );
}

/// Frozen showcase anchors, one test each so that a capability boundary hit by one fixture cannot
/// hide the state of the others.
macro_rules! showcase_anchor {
    ($test:ident, $fixture:literal, $bytes:literal, $fnv:literal) => {
        #[test]
        #[ignore = "explicit P12 showcase anchor: full horizon"]
        fn $test() {
            let image = lower($fixture);
            assert_anchor(
                $fixture,
                identical_across_local_backends($fixture, &image, None),
                $bytes,
                $fnv,
            );
        }
    };
}

showcase_anchor!(
    f_het_anchor_is_identical_across_local_backends,
    "f_het_k8_tiered_delays.toml",
    692_615,
    0xe31a_26ac_20c5_b24b
);
showcase_anchor!(
    f_burst_anchor_is_identical_across_local_backends,
    "f_burst_coflow_phases.toml",
    216_717,
    0xc0d4_21d4_59ae_a53f
);
showcase_anchor!(
    f_topo_anchor_is_identical_across_local_backends,
    "f_topo_dragonfly_g33.toml",
    7_343_196,
    0x016f_d8e5_6994_926e
);
showcase_anchor!(
    aqm_anchor_is_identical_across_local_backends,
    "f_aqm_alias_incast32.toml",
    761_961,
    0x3460_b70a_a889_a1c9
);

// -------------------------------------------------------------------------------------------
// E3 — legacy-comparability fixture.
// -------------------------------------------------------------------------------------------

#[test]
fn e3_is_a_rack_local_dominant_byte_terminated_matrix_on_default_routing() {
    for arm in ["st", "mt"] {
        let name = format!("e3_legacy_rack_local_{arm}.toml");
        let config = fixture_table(&name);
        assert!(
            config.get("routing").is_none(),
            "{name}: legacy has no equal-cost multipath, so E3 must stay on default routing"
        );
        assert_eq!(config["topology"]["fat_tree"]["k"].as_integer(), Some(32));
        assert_eq!(config["switch"]["port_rate"].as_integer(), Some(3_200_000));
        let flows = config["flow"].as_array().expect("explicit flows");
        assert_eq!(flows.len(), 16_896);
        for flow in flows {
            let traffic = &flow["traffic"];
            assert!(
                traffic.get("size").is_some() && traffic.get("duration").is_none(),
                "{name}: every flow must be byte-terminated -- legacy emits one extra packet per \
                 duration-terminated flow"
            );
        }
        // 93.8% of packets rack-local: 8,192 flows at 4,800 packets against 8,704 at 300.
        let rack_local_packets = 8_192 * 4_800;
        let other_packets = 8_704 * 300;
        let share = rack_local_packets as f64 / (rack_local_packets + other_packets) as f64;
        assert!(
            share > 0.93,
            "{name}: the matrix must stay rack-local-dominant, got {share}"
        );
    }
}

/// The re-freeze is only a re-freeze if the flows are the ones the slide preview measured.
#[test]
fn e3_generator_reproduces_the_committed_fixtures_byte_for_byte() {
    let directory = std::env::temp_dir().join(format!("days-t21-e3-{}", std::process::id()));
    std::fs::create_dir_all(&directory).expect("temporary output directory");
    let generator = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("configs/benchmarks/p12/gen_e3_rack_local.py");
    let status = std::process::Command::new("python3")
        .arg(&generator)
        .arg("--out-dir")
        .arg(&directory)
        .status()
        .expect("python3 must be available to regenerate E3");
    assert!(status.success(), "E3 generator failed");
    for arm in ["st", "mt"] {
        let name = format!("e3_legacy_rack_local_{arm}.toml");
        let regenerated = std::fs::read(directory.join(&name)).expect("regenerated fixture");
        let committed = std::fs::read(fixture_path(&name)).expect("committed fixture");
        assert!(
            regenerated == committed,
            "{name}: the committed fixture is not what the generator produces"
        );
    }
    let _ = std::fs::remove_dir_all(&directory);
}

#[test]
#[ignore = "explicit P12 E3 anchor: full 18 s simulated horizon, ~16.9k flows"]
fn e3_anchor_is_identical_across_local_backends() {
    let name = "e3_legacy_rack_local_st.toml";
    let image = lower(name);
    assert_anchor(
        name,
        identical_across_local_backends(name, &image, None),
        E3_ANCHOR_BYTES,
        E3_ANCHOR_FNV,
    );
}

/// Both E3 arms differ only in legacy threading keys, which Days AGO ignores, so the two files
/// must lower to the same image. That equality is itself part of the comparison: it is what lets
/// the legacy ST and MT rows be read against one Days AGO row.
#[test]
#[ignore = "explicit P12 E3 gate: lowers two 3.8 MB fixtures"]
fn e3_arms_lower_to_the_same_image() {
    let st = lower("e3_legacy_rack_local_st.toml");
    let mt = lower("e3_legacy_rack_local_mt.toml");
    assert_eq!(fingerprint(&st), fingerprint(&mt));
}

// Frozen scalar anchors for the two fixtures whose horizons make them explicit gates.
//
// E2 is anchored at a 50 us PROBE horizon rather than its own 1.152 ms: the fixture's horizon is
// the closed-loop measurement window and belongs to the measurer, while this gate only has to
// notice that lowering or semantics moved. E3 is anchored at its full 18 s simulated horizon
// because that horizon is cheap in events (41,932,800 packets, ~59 s of scalar wall) and because
// the packet total is itself the cross-check against the slide preview.
const E2_ANCHOR_BYTES: u64 = 218_993_720;
const E2_ANCHOR_FNV: u64 = 0x2e8b_31d8_1ccd_20f9;
const E3_ANCHOR_BYTES: u64 = 50_178_088;
const E3_ANCHOR_FNV: u64 = 0x803c_69f2_c227_23cc;

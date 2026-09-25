use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use days::scenario::compile_config;
use days_executor::{FlowGeneratorKind, GeneratorTermination, SchedulerKind, run_scalar_rounds};

const FIXTURE_DIRECTORY: &str = "configs/benchmarks/width_via_load";
const MIN_EVENTS: u128 = 10_000_000;
const MAX_EVENTS: u128 = 50_000_000;

#[derive(Clone, Copy)]
struct Fixture {
    name: &'static str,
    seed: i64,
    target_width: u64,
    generator_count: u64,
    flow_set_count: usize,
    flow_bytes: u64,
    stop_time: f64,
    cohort_flow_counts: &'static [u64],
    initial_delays: &'static [f64],
}

const FIXTURES: [Fixture; 5] = [
    Fixture {
        name: "fattree_k32_target_w01000.toml",
        seed: 13_001,
        target_width: 1_000,
        generator_count: 480,
        flow_set_count: 2,
        flow_bytes: 524_288,
        stop_time: 0.000_119,
        cohort_flow_counts: &[240, 240],
        initial_delays: &[0.0, 0.000_040],
    },
    Fixture {
        name: "fattree_k32_target_w03000.toml",
        seed: 13_003,
        target_width: 3_000,
        generator_count: 1_660,
        flow_set_count: 1,
        flow_bytes: 524_288,
        stop_time: 0.000_045,
        cohort_flow_counts: &[1_660],
        initial_delays: &[0.0],
    },
    Fixture {
        name: "fattree_k32_target_w07000.toml",
        seed: 13_007,
        target_width: 7_000,
        generator_count: 4_555,
        flow_set_count: 1,
        flow_bytes: 524_288,
        stop_time: 0.000_035,
        cohort_flow_counts: &[4_555],
        initial_delays: &[0.0],
    },
    Fixture {
        name: "fattree_k32_target_w15000.toml",
        seed: 13_015,
        target_width: 15_000,
        generator_count: 10_000,
        flow_set_count: 1,
        flow_bytes: 524_288,
        stop_time: 0.000_025,
        cohort_flow_counts: &[10_000],
        initial_delays: &[0.0],
    },
    Fixture {
        name: "fattree_k32_target_w30000.toml",
        seed: 13_030,
        target_width: 30_000,
        generator_count: 32_768,
        flow_set_count: 1,
        flow_bytes: 131_072,
        stop_time: 0.000_010,
        cohort_flow_counts: &[32_768],
        initial_delays: &[0.0],
    },
];

fn fixture_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join(FIXTURE_DIRECTORY)
        .join(name)
}

fn read_fixture(path: &Path) -> toml::Table {
    fs::read_to_string(path)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display()))
        .parse()
        .unwrap_or_else(|error| panic!("failed to parse {}: {error}", path.display()))
}

#[test]
fn width_via_load_fixtures_hold_the_t13e_design_invariants() {
    let mut previous_generator_count = 0;

    for fixture in FIXTURES {
        let path = fixture_path(fixture.name);
        let config = read_fixture(&path);
        let explicit_flow_count = config
            .get("flow")
            .map(|flows| {
                flows
                    .as_array()
                    .unwrap_or_else(|| panic!("{} flow must be a TOML array", fixture.name))
                    .len()
            })
            .unwrap_or_default();
        let flow_sets = config["flow_set"]
            .as_array()
            .expect("fixture must contain flow sets");
        let generator_count = flow_sets
            .iter()
            .map(|flow_set| {
                u64::try_from(
                    flow_set["flow_count"]
                        .as_integer()
                        .expect("flow count must be an integer"),
                )
                .expect("flow count must be positive")
            })
            .sum::<u64>();

        assert_eq!(config["seed"].as_integer(), Some(fixture.seed));
        assert_eq!(config["topology"]["category"].as_str(), Some("FatTree"));
        assert_eq!(config["topology"]["fat_tree"]["k"].as_integer(), Some(32));
        assert_eq!(
            config["switch"]["port_rate"].as_integer(),
            Some(100_000_000_000)
        );
        assert_eq!(config["switch"]["discipline"].as_str(), Some("FIFO"));
        assert_eq!(config["switch"]["drop"].as_str(), Some("TailDrop"));
        assert_eq!(config["switch"]["capacity"].as_integer(), Some(1_024));
        assert_eq!(config["link"]["propagation_ns"].as_integer(), Some(1_000));
        assert_eq!(
            config["duration"].as_float(),
            Some(fixture.stop_time),
            "{} must use its budget-derived stop time",
            fixture.name
        );
        assert_eq!(
            explicit_flow_count, 0,
            "{} must not declare explicit flows",
            fixture.name
        );
        assert_eq!(
            flow_sets.len(),
            fixture.flow_set_count,
            "{} must declare exactly {} flow sets",
            fixture.name,
            fixture.flow_set_count
        );
        assert_eq!(
            generator_count, fixture.generator_count,
            "{} must declare exactly {} flow generators",
            fixture.name, fixture.generator_count
        );
        assert_eq!(flow_sets.len(), fixture.initial_delays.len());
        assert_eq!(flow_sets.len(), fixture.cohort_flow_counts.len());
        assert!(
            generator_count > previous_generator_count,
            "offered load must increase strictly across the target-width sweep"
        );
        previous_generator_count = generator_count;

        let mut previous_initial_delay = None;
        let mut offered_bytes = 0_u64;
        for (cohort, flow_set) in flow_sets.iter().enumerate() {
            let flow_set_table = flow_set
                .as_table()
                .expect("each flow set must be a TOML table");
            let traffic = &flow_set["traffic"];
            let flow_bytes = traffic["size"]
                .as_integer()
                .expect("finite traffic must terminate by byte count");
            let initial_delay = traffic["initial_delay"]
                .as_float()
                .expect("each cohort must declare its initial delay");
            let arrival = &traffic["arr_dist"];
            let packet_size = &traffic["pkt_size_dist"];

            assert_eq!(flow_set["flow_type"].as_str(), Some("PacketDistribution"));
            for field in ["routing", "starts_before", "starts_after"] {
                assert!(
                    !flow_set_table.contains_key(field),
                    "{} cohort {cohort} must omit compiler-significant field {field}",
                    fixture.name
                );
            }
            assert_eq!(
                flow_set["flow_count"].as_integer(),
                i64::try_from(fixture.cohort_flow_counts[cohort]).ok(),
                "{} cohort {cohort} must use its exact flow count",
                fixture.name
            );
            assert!(
                traffic.get("duration").is_none(),
                "finite flows must not use duration termination"
            );
            assert_eq!(
                u64::try_from(flow_bytes).unwrap(),
                fixture.flow_bytes,
                "{} must use its event-budget-derived finite flow size",
                fixture.name
            );
            assert!(
                (131_072..=524_288).contains(&fixture.flow_bytes),
                "{} must remain in the plan's illustrative 128-512 KiB range",
                fixture.name
            );
            assert_eq!(arrival["type"].as_str(), Some("Uniform"));
            assert_eq!(arrival["low"].as_float(), Some(0.000_000_021));
            assert_eq!(arrival["high"].as_float(), Some(0.000_000_021));
            assert_eq!(packet_size["type"].as_str(), Some("Uniform"));
            assert_eq!(packet_size["low"].as_integer(), Some(256));
            assert_eq!(packet_size["high"].as_integer(), Some(256));
            assert_eq!(
                initial_delay, fixture.initial_delays[cohort],
                "{} cohort {cohort} must use its exact stagger",
                fixture.name
            );

            if let Some(previous) = previous_initial_delay {
                assert!(
                    initial_delay > previous,
                    "{} cohort delays must be strictly increasing",
                    fixture.name
                );
            }
            previous_initial_delay = Some(initial_delay);
            offered_bytes = offered_bytes
                .checked_add(
                    u64::try_from(flow_set["flow_count"].as_integer().unwrap()).unwrap()
                        * u64::try_from(flow_bytes).unwrap(),
                )
                .expect("offered byte count must fit");
        }

        assert_eq!(
            offered_bytes,
            fixture.generator_count * fixture.flow_bytes,
            "{} must offer the exact finite byte budget",
            fixture.name
        );
        assert!(
            fixture.target_width <= 30_000,
            "the fixture labels document the requested empirical width targets"
        );
    }
}

#[test]
fn smallest_width_via_load_fixture_lowers_and_truncates_pending_tail() {
    let fixture = FIXTURES[0];
    let path = fixture_path(fixture.name);
    let image = compile_config(&path)
        .unwrap_or_else(|error| panic!("failed to lower {}: {error}", path.display()));

    assert_eq!(image.stop_time_ns, 119_000);
    assert_eq!(image.seed, 13_001);
    assert_eq!(image.host_states.len(), 512);
    assert_eq!(image.switch_states.len(), 33_280);
    assert_eq!(image.nodes.len(), 33_792);
    assert!(
        image
            .switch_states
            .iter()
            .all(|switch| switch.queues.len() == 1)
    );
    assert!(
        image
            .links
            .iter()
            .all(|link| { link.rate_bps == 100_000_000_000 && link.propagation_ns == 1_000 })
    );

    let queues = image
        .switch_states
        .iter()
        .flat_map(|switch| &switch.queues)
        .collect::<Vec<_>>();
    assert_eq!(queues.len(), 33_280);
    assert!(queues.iter().all(|queue| {
        queue.scheduler == SchedulerKind::Fifo && queue.queue_capacity_packets == 1_024
    }));

    let generators = image
        .host_states
        .iter()
        .flat_map(|host| &host.generators)
        .collect::<Vec<_>>();
    assert_eq!(image.flows.len(), 480);
    assert_eq!(generators.len(), 480);
    assert!(generators.iter().all(|generator| {
        matches!(
            generator.kind,
            FlowGeneratorKind::Constant(constant)
                if constant.interval_ns == 21
                    && constant.packet_size_bytes == 256
                    && constant.termination == GeneratorTermination::Bytes(524_288)
        )
    }));
    let first_departure_counts =
        generators
            .iter()
            .fold(BTreeMap::new(), |mut counts, generator| {
                let first_departure_ns = match generator.kind {
                    FlowGeneratorKind::Constant(constant) => constant.first_departure_ns,
                    FlowGeneratorKind::Tcp(_)
                    | FlowGeneratorKind::Rate(_)
                    | FlowGeneratorKind::Collective(_)
                    | FlowGeneratorKind::Dcqcn(_) => {
                        panic!("fixture uses constant generators")
                    }
                };
                *counts.entry(first_departure_ns).or_insert(0_usize) += 1;
                counts
            });
    assert_eq!(
        first_departure_counts,
        BTreeMap::from([(0_u64, 240_usize), (40_000_u64, 240_usize)])
    );

    let run = run_scalar_rounds(&image, None)
        .unwrap_or_else(|error| panic!("failed to execute {}: {error}", path.display()));
    let total_events = run
        .rounds
        .iter()
        .map(|round| u128::from(round.events_processed))
        .sum::<u128>();

    assert!(
        (MIN_EVENTS..=MAX_EVENTS).contains(&total_events),
        "{} processed {total_events} events, outside [{MIN_EVENTS}, {MAX_EVENTS}]",
        fixture.name
    );
    assert!(
        !run.result.pending_events.is_empty(),
        "{} must retain pending work at its configured stop",
        fixture.name
    );
    assert!(
        run.result
            .pending_events
            .iter()
            .all(|event| event.key.time_ns > image.stop_time_ns),
        "{} must stop before its pending tail",
        fixture.name
    );
    assert!(
        !run.result.resident_packets.is_empty(),
        "{} must retain in-flight or queued packets at its configured stop",
        fixture.name
    );
}

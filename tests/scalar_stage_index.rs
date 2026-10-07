//! P14 scan equality gate: the keyed Scalar stage path answers every query as the retired scans.
//!
//! `days_executor::scalar::assert_scalar_stage_index_equivalent_for_testing` compares each host's
//! stage index with the retained pre-index scans (`stage_index::legacy_scans`) on the image, and,
//! after every event of a Scalar run, compares the targeted host's maintained index with a fresh
//! derivation and with the scans. Debug builds also compare every activation choice with the scan
//! in the middle of the event that makes it. This file runs that gate over the collective and
//! compute fixtures, over running checkpoints of them, and over tables the validator rejects
//! (duplicate flows, a table out of flow order, a stage releasable at load), where the index must
//! still return the scan's answer.
//!
//! Run: `cargo test -p days --features test --test scalar_stage_index`.
#![cfg(feature = "test")]

#[path = "collective_tcp.rs"]
#[allow(dead_code)]
mod tcp;

use std::path::PathBuf;

use days::scenario::compile_config;
use days_executor::scalar::assert_scalar_stage_index_equivalent_for_testing;
use days_executor::{
    Backend, CpuConfig, GeneratorStatus, HostState, ObservationMode, RunResult, SimulationImage,
    StageRole, run_cpu_with_observations, run_scalar_with_observations, validate,
};

fn ring_allgather_compute_config(ranks: u64) -> String {
    let hosts = (0..ranks)
        .map(|host| host.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let gather = tcp::tcp_collective_config("AllGather", ranks, ranks * 1_500, 100);
    let gather = &gather[gather.find("[[collective]]").expect("collective block")..];
    tcp::tcp_collective_config("RingAllReduce", ranks, ranks * 2_500, 100).replace(
        "[[collective]]\n",
        "[[collective]]\nname = \"grad\"\nafter = \"forward\"\n",
    ) + &gather.replace(
        "[[collective]]\n",
        "[[collective]]\nname = \"gather\"\nafter = \"backward\"\n",
    ) + &format!(
        r#"
[[compute]]
name = "forward"
hosts = [{hosts}]
duration_ns = 5000

[[compute]]
name = "backward"
hosts = [{hosts}]
duration_ns = 7000
after = "grad"

[[compute]]
name = "optimizer"
hosts = [{hosts}]
duration_ns = 3000
after = "gather"
"#
    )
}

fn compute_chain_config() -> String {
    tcp::tcp_collective_config("RingAllReduce", 3, 9_001, 100).replace(
        "[[collective]]\n",
        "[[collective]]\nname = \"grad\"\nafter = \"forward\"\n",
    ) + r#"
[[compute]]
name = "forward"
hosts = [0, 1, 2]
duration_ns = 5000

[[compute]]
name = "backward"
hosts = [0, 1, 2]
duration_ns = 7000
after = "grad"
"#
}

fn lossy_ring_config() -> String {
    tcp::tcp_collective_config("RingAllReduce", 4, 20_000, 4)
        .replace("duration = 0.05", "duration = 5.0")
        .replace(
            "edges = [[0, 4], [1, 4], [2, 4], [3, 4]]",
            "edges = [[0, 4], [1, 4], [2, 5], [3, 5], [4, 5]]",
        )
        .replace("sources = [0, 1, 2, 3]", "sources = [0, 2, 1, 3]")
        .replace("sinks = [1, 2, 3, 0]", "sinks = [2, 1, 3, 0]")
}

/// Collective and compute fixtures over TCP, with and without loss.
fn stage_fixtures() -> Vec<(String, SimulationImage)> {
    let mut fixtures = Vec::new();
    for (algorithm, ranks, size) in [
        ("RingAllReduce", 4, 10_001),
        ("AllGather", 4, 10_001),
        ("RingAllReduce", 8, 8_000),
        ("AllGather", 8, 8_000),
    ] {
        let label = format!("{algorithm}-{ranks}");
        let config = tcp::tcp_collective_config(algorithm, ranks, size, 100);
        fixtures.push((label.clone(), tcp::compile_text(&label, &config)));
    }
    fixtures.push((
        "compute-chain".to_owned(),
        tcp::compile_text("compute-chain", &compute_chain_config()),
    ));
    for ranks in [4, 8] {
        let label = format!("ring-allgather-compute-{ranks}");
        fixtures.push((
            label.clone(),
            tcp::compile_text(&label, &ring_allgather_compute_config(ranks)),
        ));
    }
    fixtures.push((
        "lossy-ring".to_owned(),
        tcp::compile_text("lossy-ring", &lossy_ring_config()),
    ));
    fixtures
}

fn stage_count(image: &SimulationImage) -> usize {
    image
        .host_states
        .iter()
        .flat_map(|state| &state.stages)
        .flatten()
        .count()
}

/// Runs the gate and returns the run's outcome, which must equal the ordinary Scalar run's.
fn check(label: &str, image: &SimulationImage) -> Result<RunResult, String> {
    let checked = assert_scalar_stage_index_equivalent_for_testing(image, None)
        .unwrap_or_else(|mismatch| panic!("{label}: {mismatch}"));
    let plain = run_scalar_with_observations(image, None, ObservationMode::Full);
    match (checked, plain) {
        (Ok(checked), Ok(plain)) => {
            assert!(
                checked == plain,
                "{label}: the checked run changed the result"
            );
            Ok(checked)
        }
        (Err(checked), Err(plain)) => {
            assert_eq!(checked, plain, "{label}: the checked run changed the error");
            Err(checked.to_string())
        }
        (checked, plain) => panic!(
            "{label}: checked run {:?} and plain run {:?} disagree on success",
            checked.is_ok(),
            plain.is_ok()
        ),
    }
}

fn checkpoint_after(source: &SimulationImage, label: &str, events: u64) -> SimulationImage {
    let prefix = run_scalar_with_observations(source, Some(events), ObservationMode::Full)
        .unwrap_or_else(|error| panic!("{label}: prefix run failed: {error}"));
    let mut checkpoint = source.clone();
    checkpoint.host_states = prefix.host_states;
    checkpoint.switch_states = prefix.switch_states;
    checkpoint.initial_packets = prefix.resident_packets;
    checkpoint.initial_events = prefix.pending_events;
    checkpoint
}

#[test]
fn stage_index_matches_the_scans_on_collective_and_compute_fixtures() {
    for (label, image) in stage_fixtures() {
        assert!(stage_count(&image) > 0, "{label}: must carry stages");
        validate(&image, Backend::Scalar).unwrap_or_else(|error| panic!("{label}: {error}"));
        let result = check(&label, &image).unwrap_or_else(|error| panic!("{label}: {error}"));
        let unfinished = result
            .host_states
            .iter()
            .flat_map(HostState::generators_with_stages)
            .filter(|(_, stage)| stage.is_some())
            .filter(|(generator, _)| generator.next_emission.status != GeneratorStatus::Finished)
            .count();
        assert_eq!(unfinished, 0, "{label}: every stage must finish");
    }
}

#[test]
fn stage_index_matches_the_scans_on_running_checkpoints() {
    let mut released_mid_run = 0_usize;
    for (label, source) in stage_fixtures() {
        // Event times, not counts: the prefix horizon is exclusive in simulated nanoseconds.
        for horizon_ns in [1_u64, 2_000, 8_000, 20_000, 60_000] {
            let checkpoint = checkpoint_after(&source, &label, horizon_ns);
            let partly_released = checkpoint
                .host_states
                .iter()
                .flat_map(|state| &state.stages)
                .flatten()
                .any(|stage| stage.activated)
                && checkpoint
                    .host_states
                    .iter()
                    .flat_map(|state| &state.stages)
                    .flatten()
                    .any(|stage| !stage.activated);
            released_mid_run += usize::from(partly_released);
            let label = format!("{label} at {horizon_ns} ns");
            validate(&checkpoint, Backend::Scalar)
                .unwrap_or_else(|error| panic!("{label}: {error}"));
            check(&label, &checkpoint).unwrap_or_else(|error| panic!("{label}: {error}"));
        }
    }
    assert!(
        released_mid_run > 0,
        "the checkpoint corpus must hold stages both released and unreleased"
    );
}

#[test]
fn stage_index_matches_the_scans_on_a_non_collective_tcp_fixture() {
    let relative = "configs/benchmarks/tcp/fattree_k4_tcp_cubic_f16_smoke.toml";
    let image = compile_config(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(relative))
        .unwrap_or_else(|error| panic!("{relative}: {error}"));
    check(relative, &image).unwrap_or_else(|error| panic!("{relative}: {error}"));
}

/// The first host slot with at least two stage generators.
fn stage_host(image: &SimulationImage) -> usize {
    image
        .host_states
        .iter()
        .position(|state| state.stages.iter().flatten().count() >= 2)
        .expect("a host with two stages")
}

/// The ring image with `mutate` applied to its first host with two stages. A mutation that adds or
/// moves a generator moves its stage-table entry with it, so the table stays parallel.
fn mutated(label: &str, mutate: impl FnOnce(&mut HostState)) -> SimulationImage {
    let mut image = tcp::compile_text(label, &ring_allgather_compute_config(4));
    let slot = stage_host(&image);
    mutate(&mut image.host_states[slot]);
    image
}

/// Tables the validator rejects: the index must still give the scans' answers, event by event, and
/// the run must end exactly as the ordinary run does, success or failure.
#[test]
fn stage_index_matches_the_scans_on_tables_the_validator_rejects() {
    let cases: Vec<(&str, SimulationImage)> = vec![
        (
            "duplicate stage generator appended",
            mutated("dup-appended", |state| {
                let position = state
                    .stages
                    .iter()
                    .position(Option::is_some)
                    .expect("a stage");
                let (generator, stage) = (state.generators[position], state.stages[position]);
                state.generators.push(generator);
                state.stages.push(stage);
            }),
        ),
        (
            "duplicate stage generator adjacent",
            mutated("dup-adjacent", |state| {
                let position = state
                    .stages
                    .iter()
                    .rposition(Option::is_some)
                    .expect("a stage");
                let (generator, stage) = (state.generators[position], state.stages[position]);
                state.generators.insert(position, generator);
                state.stages.insert(position, stage);
            }),
        ),
        (
            "table out of flow order",
            mutated("reversed", |state| {
                state.generators.reverse();
                state.stages.reverse();
            }),
        ),
        (
            "stage releasable at load",
            mutated("pre-released", |state| {
                let stage = state
                    .stages
                    .iter_mut()
                    .rev()
                    .flatten()
                    .find(|stage| matches!(stage.role, StageRole::Collective(_)))
                    .expect("a collective stage");
                stage.dependencies.local_completed = stage.dependencies.local.count();
                stage.dependencies.inbound_bytes_received =
                    stage.dependencies.inbound_predecessor_bytes;
            }),
        ),
    ];
    for (label, image) in cases {
        assert!(
            validate(&image, Backend::Scalar).is_err(),
            "{label}: the case must be one the validator rejects"
        );
        let outcome = match check(label, &image) {
            Ok(result) => format!("completed pending_events={}", result.pending_events.len()),
            Err(error) => format!("error=\"{error}\""),
        };
        println!("record=scalar_stage_index case=\"{label}\" {outcome}");
    }
}

/// The CPU executor runs the same transitions over per-LP index copies; its results stay equal to
/// Scalar's at every worker count.
#[test]
fn stage_fixtures_stay_scalar_cpu_byte_identical() {
    for (label, image) in stage_fixtures() {
        let scalar = run_scalar_with_observations(&image, None, ObservationMode::Full)
            .unwrap_or_else(|error| panic!("{label}: {error}"));
        for workers in [1, 2, 4] {
            validate(&image, Backend::Cpu { workers })
                .unwrap_or_else(|error| panic!("{label}: {error}"));
            let cpu = run_cpu_with_observations(
                &image,
                None,
                CpuConfig {
                    workers,
                    ..CpuConfig::default()
                },
                ObservationMode::Full,
            )
            .unwrap_or_else(|error| panic!("{label} w{workers}: {error}"));
            assert!(
                scalar == cpu.result,
                "{label}: CPU w{workers} differs from Scalar"
            );
        }
    }
}

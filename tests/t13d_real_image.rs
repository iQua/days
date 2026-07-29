use std::fs;
use std::path::PathBuf;

use days::scenario::compile_config;
use days_executor::{Backend, validate};
#[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
use days_executor::{CpuConfig, ReplayTraceCapture, run_cpu, run_scalar_rounds_with_replay_trace};

const FIXTURE: &str = "configs/benchmarks/real_image_gate/fattree_k64_f32768_st.toml";

#[test]
fn k64_real_image_gate_fixture_preserves_the_corpus_progression() {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(FIXTURE);
    let contents = fs::read_to_string(&path).expect("T13d k64 fixture should exist");
    let config = contents
        .parse::<toml::Table>()
        .expect("T13d k64 fixture should parse");

    assert_eq!(config["topology"]["fat_tree"]["k"].as_integer(), Some(64));
    assert_eq!(
        config["flow_set"][0]["flow_count"].as_integer(),
        Some(32_768)
    );
    assert_eq!(config["switch"]["discipline"].as_str(), Some("FIFO"));
    assert_eq!(config["switch"]["drop"].as_str(), Some("TailDrop"));
}

#[test]
#[ignore = "explicit large-corpus lowering gate"]
fn k64_real_image_gate_fixture_lowers_and_validates() {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(FIXTURE);
    let image = compile_config(path).expect("T13d k64 fixture should lower");

    assert_eq!(image.nodes.len(), 266_240);
    assert_eq!(image.links.len(), 266_240);
    assert_eq!(image.flows.len(), 32_768);
    assert_eq!(image.initial_packets.len(), 32_768);
    assert_eq!(image.initial_events.len(), 32_768);
    validate(&image, Backend::Scalar).expect("k64 image should validate for scalar");
    validate(&image, Backend::Cpu { workers: 4 }).expect("k64 image should validate for CPU");
}

#[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
#[test]
fn real_round_trace_matches_the_canonical_metrics_and_cpu_result() {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("configs/benchmarks/baseline/fattree_k4_f8_st.toml");
    let image = compile_config(path).expect("k4 corpus anchor should lower");
    let (scalar, trace) = run_scalar_rounds_with_replay_trace(
        &image,
        None,
        ReplayTraceCapture {
            start_round: 4,
            rounds: 8,
        },
    )
    .expect("the canonical round path should record a real replay trace");

    trace
        .validate()
        .expect("the recorded CSR trace should validate");
    assert_eq!(trace.rounds.len(), 8);
    for replay in &trace.rounds {
        let semantic = &scalar.rounds[replay.source_round];
        let rows = &trace.lps[replay.lp_start..replay.lp_start + replay.lp_count];
        assert_eq!(replay.events_processed, semantic.events_processed);
        assert_eq!(replay.active_lp_count, semantic.active_lp_count);
        assert_eq!(
            usize::try_from(replay.maximum_events_per_lp).unwrap(),
            semantic
                .lp_work
                .iter()
                .map(|work| work.events_processed as usize)
                .max()
                .unwrap_or(0)
        );
        assert_eq!(replay.parallel_efficiency, semantic.parallel_efficiency);
        for row in rows {
            let work = semantic
                .lp_work
                .iter()
                .find(|work| work.node == row.node)
                .expect("every replay LP should have semantic work");
            let steps = &trace.steps[row.step_start..row.step_start + row.step_count];
            assert_eq!(steps.len() as u64, work.events_processed);
            assert_eq!(
                steps
                    .iter()
                    .filter(|step| step.is_direct_continuation())
                    .count() as u64,
                work.same_time_continuations
            );
            assert!(steps.iter().all(|step| {
                (step.kind() == days_executor::EventKind::TxReady)
                    == step.queue_occupancy().is_some()
            }));
        }
    }

    let cpu = run_cpu(
        &image,
        None,
        CpuConfig {
            workers: 4,
            ..CpuConfig::default()
        },
    )
    .expect("the same image should complete on W4");
    assert_eq!(scalar.result, cpu.result);
}

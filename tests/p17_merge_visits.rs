//! P17 merge: the device exchange merge's work follows each LP's inbound channels, not the
//! streams it owns (`days-gpu/evidence/P17/merge/design.md`, ruling R1 (d)).
//!
//! Before P17 the merge rebuilt every LP's active list from scratch each round, reading every
//! stream the LP declared: its inbound channels, its service stream and one generator stream per
//! flow it sources (14,443 per host on the flagship). Processing already keeps the list exact for
//! local pushes and pops; only the scatter's remote delivery leaves a channel that went from empty
//! to non-empty without an entry. The merge now reads only the LP's inbound channels and the
//! service entry that ends that prefix of its declared list, and appends the channels that the
//! round's scatter made non-empty.
//!
//! With the test hooks the device counts, per LP, the merge's invocations and the declared-list
//! entries it read; the hooks-only audit (checked by the host after every successful attempt)
//! proves each list equals the full rebuild's set. The fixtures exercise plain flows, queue-pair
//! timers in the fallback heap (the heap entry's index-0 pin), and stage notifies on the rail.

#![cfg(any(
    feature = "cuda-test-hooks",
    all(feature = "metal-test-hooks", target_vendor = "apple")
))]

use std::collections::BTreeSet;
use std::path::PathBuf;

use days::scenario::compile_config;
use days_executor::{
    MergeAuditRow, ObservationMode, RunResult, SimulationImage, run_scalar_with_observations,
};

const FIXTURES: [&str; 3] = [
    "configs/benchmarks/baseline/fattree_k4_f8_st.toml",
    "configs/p15/roce_timeout.toml",
    "configs/p16/rail_mini_roce.toml",
];

fn lower(relative: &str) -> SimulationImage {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(relative);
    compile_config(&path).unwrap_or_else(|error| panic!("{} must lower: {error}", path.display()))
}

fn scalar(image: &SimulationImage) -> RunResult {
    let mut expected = run_scalar_with_observations(image, None, ObservationMode::Full)
        .expect("scalar oracle must run");
    expected.diagnostics = None;
    expected
}

/// Inbound channels per LP.
fn inbound(image: &SimulationImage) -> Vec<u64> {
    let mut counts = vec![0_u64; image.nodes.len()];
    for channel in &image.channels {
        counts[channel.target.0 as usize] += 1;
    }
    counts
}

/// Distinct generator streams per LP: one per flow the LP sources and generates.
fn generator_streams(image: &SimulationImage) -> Vec<u64> {
    let mut flows = vec![BTreeSet::new(); image.nodes.len()];
    for state in &image.host_states {
        for generator in &state.generators {
            let source = image.flows[generator.flow.0 as usize].source.0 as usize;
            flows[source].insert(generator.flow.0);
        }
    }
    flows.iter().map(|set| set.len() as u64).collect()
}

fn assert_visits(backend: &str, fixture: &str, image: &SimulationImage, rows: &[MergeAuditRow]) {
    let inbound = inbound(image);
    let generators = generator_streams(image);
    assert_eq!(
        rows.len(),
        image.nodes.len(),
        "{backend} {fixture}: one row per LP"
    );
    let invocations = rows[0].invocations;
    assert!(invocations > 0, "{backend} {fixture}: the merge ran");
    assert!(
        generators.iter().any(|&count| count > 0),
        "{backend} {fixture}: some LP sources a generator, so its declared streams exceed its \
         inbound channels plus one"
    );
    let mut read = 0_u64;
    let mut declared_reads = 0_u64;
    for (lp, row) in rows.iter().enumerate() {
        assert_eq!(
            row.invocations, invocations,
            "{backend} {fixture}: every LP merges once per round (LP {lp})"
        );
        assert_eq!(
            row.streams_read,
            row.invocations * (inbound[lp] + 1),
            "{backend} {fixture}: LP {lp} reads its {} inbound channels and the service entry per \
             merge, not its {} declared streams",
            inbound[lp],
            inbound[lp] + 1 + generators[lp],
        );
        read += row.streams_read;
        declared_reads += row.invocations * (inbound[lp] + 1 + generators[lp]);
    }
    let appended = rows.iter().map(|row| row.appended).sum::<u64>();
    let entries = rows.iter().map(|row| row.stream_entries).sum::<u64>();
    assert!(
        appended > 0,
        "{backend} {fixture}: remote delivery activated channels"
    );
    eprintln!(
        "record=merge_visits backend={backend} fixture={fixture} lps={} merges_per_lp={invocations} \
         streams_read={read} full_rebuild_reads={declared_reads} appended={appended} \
         stream_entries={entries}",
        rows.len(),
    );
}

fn check(backend: &str, run: impl Fn(&SimulationImage) -> RunResult) {
    for fixture in FIXTURES {
        let image = lower(fixture);
        assert_eq!(
            run(&image),
            scalar(&image),
            "{backend} {fixture}: byte identity"
        );
        let rows = days_executor::take_merge_audit_for_testing()
            .expect("the device run recorded its merge rows");
        assert_visits(backend, fixture, &image, &rows);
    }
}

#[cfg(feature = "cuda-test-hooks")]
#[test]
fn cuda_merge_reads_inbound_channels_not_declared_streams() {
    check("cuda", |image| {
        days_executor::run_cuda_with_observations(
            image,
            None,
            days_executor::CudaConfig::default(),
            ObservationMode::Full,
        )
        .expect("CUDA run")
        .result
    });
}

#[cfg(all(feature = "metal-test-hooks", target_vendor = "apple"))]
#[test]
fn metal_merge_reads_inbound_channels_not_declared_streams() {
    check("metal", |image| {
        days_executor::run_metal_with_observations(
            image,
            None,
            days_executor::MetalConfig::default(),
            ObservationMode::Full,
        )
        .expect("Metal run")
        .result
    });
}

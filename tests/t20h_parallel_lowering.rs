//! T20h: per-flow route computation runs on several host threads during lowering.
//!
//! A route is a pure function of the canonical topology graph and the flow endpoints, and the
//! workers scatter their results into index-addressed slots, so the host-thread budget must be
//! invisible in the lowered image. These gates lower the same fixture under several budgets and
//! require byte-identical `SimulationImage` values, identical Debug byte counts, and identical
//! FNV-1a-64 fingerprints over those Debug bytes. Three fixtures additionally carry the frozen
//! pre-T20h lowering hashes, so a parallel scatter that silently reordered a route would be caught
//! against bytes that predate this task.

use std::fmt;
use std::path::PathBuf;

use days::scenario::{compile_config, compile_config_with_route_workers};
use days::topos::route::{RouteWorkers, route_chunk_count, route_chunk_len};
use days_executor::SimulationImage;

/// `(fixture, expected flow count, frozen pre-T20h `{:#?}` FNV-1a-64 when one exists)`.
///
/// The frozen values are the ones pinned by
/// `legacy/tests/scenario_lowering.rs::representative_lowered_image_bytes_match_frozen_preoptimization_hashes`.
const DEFAULT_CORPUS: &[(&str, usize, Option<u64>)] = &[
    (
        "configs/benchmarks/baseline/fattree_k4_f8_st.toml",
        8,
        Some(8_218_538_847_115_646_020),
    ),
    (
        "configs/benchmarks/baseline/fattree_k8_f64_st.toml",
        64,
        Some(8_308_880_521_678_431_806),
    ),
    (
        "configs/benchmarks/tcp/fattree_k4_tcp_cubic_f16_smoke.toml",
        16,
        None,
    ),
    (
        "configs/benchmarks/tcp/fattree_k16_tcp_reno_f1024.toml",
        1_024,
        None,
    ),
    (
        "configs/benchmarks/width_via_load_full/fattree_k32_load_10.toml",
        841,
        Some(18_053_182_671_785_655_793),
    ),
];

const K32_TCP_FIXTURE: &str = "configs/benchmarks/tcp/fattree_k32_tcp_reno_f8192.toml";
const FRONTIER_FIXTURE: &str = "configs/benchmarks/p11/rq9_frontier_closed_k32.toml";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ImageFingerprint {
    flows: usize,
    debug_bytes: usize,
    debug_fnv1a64: u64,
}

/// Streams `{:#?}` through an FNV-1a-64 accumulator without materializing the Debug text.
///
/// The frontier image formats to hundreds of megabytes, so the fingerprint must never be taken
/// over an intermediate `String`.
fn fingerprint(image: &SimulationImage) -> ImageFingerprint {
    struct Fnv1a64 {
        hash: u64,
        bytes: usize,
    }

    impl fmt::Write for Fnv1a64 {
        fn write_str(&mut self, value: &str) -> fmt::Result {
            self.bytes += value.len();
            self.hash = value.bytes().fold(self.hash, |hash, byte| {
                (hash ^ u64::from(byte)).wrapping_mul(0x100_0000_01b3)
            });
            Ok(())
        }
    }

    let mut accumulator = Fnv1a64 {
        hash: 0xcbf2_9ce4_8422_2325,
        bytes: 0,
    };
    fmt::write(&mut accumulator, format_args!("{image:#?}"))
        .expect("formatting into the image hasher should succeed");
    ImageFingerprint {
        flows: image.flows.len(),
        debug_bytes: accumulator.bytes,
        debug_fnv1a64: accumulator.hash,
    }
}

fn fixture_path(fixture: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(fixture)
}

fn lower(fixture: &str, workers: RouteWorkers) -> SimulationImage {
    compile_config_with_route_workers(fixture_path(fixture), workers)
        .unwrap_or_else(|error| panic!("T20h fixture {fixture} should lower: {error}"))
}

fn report(fixture: &str, label: &str, workers: RouteWorkers, print: ImageFingerprint) {
    println!(
        "record=p11_t20h_equality fixture={fixture} arm={label} route_workers={} chunks={} \
         flows={} debug_bytes={} debug_fnv1a64={}",
        workers.get(),
        route_chunk_count(print.flows, workers),
        print.flows,
        print.debug_bytes,
        print.debug_fnv1a64,
    );
}

/// Lowers `fixture` serially and under every parallel budget, and requires identical bytes.
fn assert_budget_invariant_lowering(
    fixture: &str,
    expected_flows: usize,
    frozen_fnv1a64: Option<u64>,
    parallel_budgets: &[RouteWorkers],
) -> ImageFingerprint {
    let serial = lower(fixture, RouteWorkers::serial());
    assert_eq!(
        serial.flows.len(),
        expected_flows,
        "T20h fixture {fixture} must keep its declared flow count"
    );
    assert_eq!(
        route_chunk_count(serial.flows.len(), RouteWorkers::serial()),
        1,
        "the serial reference must never partition"
    );
    let serial_print = fingerprint(&serial);
    report(fixture, "serial", RouteWorkers::serial(), serial_print);

    if let Some(expected) = frozen_fnv1a64 {
        assert_eq!(
            serial_print.debug_fnv1a64, expected,
            "T20h fixture {fixture} must still lower to its frozen pre-T20h image bytes"
        );
    }

    for &workers in parallel_budgets {
        assert!(
            route_chunk_count(expected_flows, workers) >= 2,
            "budget {} must actually partition {expected_flows} flows, otherwise the gate is vacuous",
            workers.get()
        );
        let parallel = lower(fixture, workers);
        let parallel_print = fingerprint(&parallel);
        report(fixture, "parallel", workers, parallel_print);
        assert_eq!(
            parallel,
            serial,
            "T20h fixture {fixture} lowered under {} route workers must be byte-identical to serial",
            workers.get()
        );
        assert_eq!(
            parallel_print, serial_print,
            "T20h fixture {fixture} Debug fingerprint must not depend on the route worker budget"
        );
    }

    serial_print
}

#[test]
fn route_partitions_cover_every_flow_index_exactly_once() {
    for flow_count in [0_usize, 1, 2, 3, 7, 8, 15, 16, 17, 63, 64, 1_024, 262_144] {
        for budget in [1_usize, 2, 3, 4, 5, 7, 16, 18, 64, 1_000] {
            let workers = RouteWorkers::new(budget);
            let chunk_len = route_chunk_len(flow_count, workers);
            assert!(chunk_len >= 1, "a chunk must never be empty");
            let chunks = route_chunk_count(flow_count, workers);
            assert_eq!(
                chunks,
                flow_count.div_ceil(chunk_len),
                "the chunk count must match the contiguous partition of {flow_count} flows"
            );
            assert!(
                chunks <= workers.get().max(1),
                "the partition must never exceed the {budget}-worker budget"
            );

            let mut covered = 0_usize;
            for chunk in 0..chunks {
                let start = chunk * chunk_len;
                let end = ((chunk + 1) * chunk_len).min(flow_count);
                assert_eq!(
                    start, covered,
                    "chunk {chunk} must start where the previous ended"
                );
                assert!(start < end, "chunk {chunk} must be non-empty");
                covered = end;
            }
            assert_eq!(
                covered, flow_count,
                "the partition of {flow_count} flows under {budget} workers must be exhaustive"
            );
        }
    }
}

#[test]
fn route_worker_budget_is_clamped_to_a_usable_range() {
    assert_eq!(RouteWorkers::serial().get(), 1);
    assert_eq!(
        RouteWorkers::new(0).get(),
        1,
        "a zero budget must still lower"
    );
    assert_eq!(RouteWorkers::new(7).get(), 7);
    assert_eq!(
        RouteWorkers::new(usize::MAX).get(),
        days::topos::route::MAX_ROUTE_WORKERS,
        "an unbounded budget must be capped"
    );
    assert!(RouteWorkers::available().get() >= 1);
    assert!(RouteWorkers::available().get() <= days::topos::route::MAX_ROUTE_WORKERS);
}

#[test]
fn parallel_route_scatter_lowers_the_default_corpus_byte_identically() {
    for &(fixture, expected_flows, frozen) in DEFAULT_CORPUS {
        let serial_print = assert_budget_invariant_lowering(
            fixture,
            expected_flows,
            frozen,
            &[
                RouteWorkers::new(2),
                RouteWorkers::new(3),
                RouteWorkers::new(4),
            ],
        );

        // The default entry point must select a budget without changing a single image byte.
        let default = compile_config(fixture_path(fixture))
            .unwrap_or_else(|error| panic!("T20h fixture {fixture} should lower: {error}"));
        assert_eq!(
            fingerprint(&default),
            serial_print,
            "compile_config must lower {fixture} to the serial reference bytes"
        );
    }
}

#[test]
#[ignore = "explicit T20h k32 TCP equality gate"]
fn parallel_route_scatter_lowers_the_k32_tcp_corpus_byte_identically() {
    assert_budget_invariant_lowering(
        K32_TCP_FIXTURE,
        8_192,
        None,
        &[RouteWorkers::new(4), RouteWorkers::available()],
    );
}

#[test]
#[ignore = "explicit T20h frontier-scale equality gate"]
fn parallel_route_scatter_lowers_the_frontier_byte_identically() {
    assert_budget_invariant_lowering(
        FRONTIER_FIXTURE,
        262_144,
        None,
        &[RouteWorkers::available()],
    );
}

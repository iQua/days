#![cfg(all(feature = "metal-test-hooks", target_vendor = "apple"))]

use std::path::PathBuf;

use days::scenario::compile_config;
use days_executor::{
    ArenaOccupancyHighWater, DeviceCapacityCaps, MetalConfig, MetalExecutor, ObservationMode,
    last_plane_words_for_testing, run_scalar_with_observations, size_metal_plan_for_testing,
    take_dominant_arena_high_water_for_testing,
};

const FRONTIER_CAPS: DeviceCapacityCaps = DeviceCapacityCaps {
    fallback_fel_events_per_lp: Some(16_384),
    queue_packets_per_lp: Some(2_048),
    channel_events_per_stream: Some(2_048),
    remote_staging_events_per_lp: Some(2_048),
    outbox_events_total: Some(2_000_000),
    tcp_receiver_ranges_per_flow: Some(64),
    tcp_ledger_segments_per_flow: Some(4_096),
    observation_events_per_lp: Some(512),
};

fn smoke_image() -> days_executor::SimulationImage {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("configs/benchmarks/tcp/fattree_k4_tcp_cubic_f16_smoke.toml");
    compile_config(&path).unwrap_or_else(|error| panic!("{} must lower: {error}", path.display()))
}

fn p12_image(name: &str) -> days_executor::SimulationImage {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("configs/benchmarks/p12")
        .join(name);
    compile_config(&path).unwrap_or_else(|error| panic!("{} must lower: {error}", path.display()))
}

fn print_arena(label: &str, occupancy: &ArenaOccupancyHighWater) {
    let (peak_entity, peak) = occupancy
        .high_water
        .iter()
        .copied()
        .enumerate()
        .max_by_key(|(_, value)| *value)
        .expect("fixture arena must have entities");
    let (utilized_entity, utilized, utilized_capacity) = occupancy
        .high_water
        .iter()
        .copied()
        .zip(occupancy.capacities.iter().copied())
        .enumerate()
        .max_by(|(_, (left, left_capacity)), (_, (right, right_capacity))| {
            u128::from(*left)
                .saturating_mul(u128::from(*right_capacity))
                .cmp(&u128::from(*right).saturating_mul(u128::from(*left_capacity)))
        })
        .map(|(entity, (observed, capacity))| (entity, observed, capacity))
        .expect("fixture arena must have entities");
    println!(
        "{label}: peak={peak} entity={peak_entity} bound_at_peak={} max_utilization={utilized}/{utilized_capacity} entity={utilized_entity}",
        occupancy.capacities[peak_entity]
    );
}

fn measure_full_fixture(name: &str) {
    let image = p12_image(name);
    let expected = run_scalar_with_observations(&image, None, ObservationMode::Summary)
        .expect("scalar completion run must succeed");
    let executor = MetalExecutor::new().expect("Metal executor must initialize");
    let actual = executor
        .run_with_observations(
            &image,
            None,
            MetalConfig {
                capacity_caps: FRONTIER_CAPS,
                ..MetalConfig::default()
            },
            ObservationMode::Summary,
        )
        .unwrap_or_else(|error| panic!("{name} capped Metal completion must succeed: {error}"));
    assert_eq!(actual.result, expected, "{name}: Metal must match scalar");
    assert!(
        actual.capacity_retry_trace.is_empty(),
        "{name}: the capped plan must run without retry: {:?}",
        actual.capacity_retry_trace
    );
    let high_water = take_dominant_arena_high_water_for_testing()
        .expect("a completed instrumented attempt must publish high-water vectors");
    for (arena, occupancy) in [
        ("stream_records", &high_water.stream_records),
        ("remote_staging", &high_water.remote_staging),
        ("queue_records", &high_water.queue_records),
    ] {
        assert!(
            occupancy
                .high_water
                .iter()
                .zip(&occupancy.capacities)
                .all(|(observed, capacity)| observed <= capacity),
            "{name} {arena}: observed occupancy exceeded the planned bound"
        );
        print_arena(arena, occupancy);
    }
}

#[test]
fn dominant_arena_high_water_is_test_only_and_fingerprint_neutral() {
    let image = smoke_image();
    let before = size_metal_plan_for_testing(
        &image,
        None,
        MetalConfig::default(),
        ObservationMode::Summary,
    )
    .expect("plan must size before the instrumented run");
    let expected = run_scalar_with_observations(&image, None, ObservationMode::Summary)
        .expect("scalar smoke run must succeed");

    let executor = MetalExecutor::new().expect("Metal executor must initialize");
    let actual = executor
        .run_with_observations(
            &image,
            None,
            MetalConfig::default(),
            ObservationMode::Summary,
        )
        .expect("Metal smoke run must succeed");
    let high_water = take_dominant_arena_high_water_for_testing()
        .expect("a completed instrumented attempt must publish its high-water marks");

    assert_eq!(
        actual.result, expected,
        "the hook must not touch complete state"
    );
    assert_eq!(
        last_plane_words_for_testing(),
        (before.total_device_bytes / std::mem::size_of::<u64>()) as u64,
        "test-only metadata tails must not count as planned device words"
    );
    assert_eq!(
        high_water.stream_records.high_water.len(),
        image.channels.len() + image.nodes.len() + image.flows.len()
    );
    assert_eq!(
        high_water.remote_staging.high_water.len(),
        image.nodes.len()
    );
    assert_eq!(high_water.queue_records.high_water.len(), image.nodes.len());
    for (arena, occupancy) in [
        ("stream_records", &high_water.stream_records),
        ("remote_staging", &high_water.remote_staging),
        ("queue_records", &high_water.queue_records),
    ] {
        assert_eq!(
            occupancy.high_water.len(),
            occupancy.capacities.len(),
            "{arena}: one high-water per planned entity"
        );
        assert!(
            occupancy.high_water.iter().any(|value| *value > 0),
            "{arena}: the TCP smoke run must exercise the arena"
        );
        assert!(
            occupancy
                .high_water
                .iter()
                .zip(&occupancy.capacities)
                .all(|(observed, capacity)| observed <= capacity),
            "{arena}: successful execution cannot exceed a planned bound"
        );
    }

    let after = size_metal_plan_for_testing(
        &image,
        None,
        MetalConfig::default(),
        ObservationMode::Summary,
    )
    .expect("plan must size after the instrumented run");
    assert_eq!(after, before, "the hook must add no planned device bytes");
}

#[test]
#[ignore = "explicit P12 E4 pre-bound occupancy measurement: full completion"]
fn e4_capped_completion_reports_dominant_arena_high_water() {
    measure_full_fixture("e4_gedes_native_k32.toml");
}

#[test]
#[ignore = "explicit P12 E2 pre-bound occupancy measurement: full completion"]
fn e2_capped_completion_reports_dominant_arena_high_water() {
    measure_full_fixture("e2_closed_k32_tcp_reno.toml");
}
